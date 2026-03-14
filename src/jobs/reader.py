from threading import Thread, Event
from queue import Queue
from logging import getLogger

import time

import serial
from gpiozero.pins.mock import MockFactory
from gpiozero.exc import BadPinFactory
from gpiozero import OutputDevice, Device

from src.exception.mcu_no_response import MCUNoResponse
from src.settings import Settings
from src.structs.sample import Sample
from src.structs.mcu_settings import MCUSettingsFrame

logger = getLogger(__name__)


class Reader(Thread):
    def __init__(self, settings: Settings, queues: list[Queue], shutdown_event: Event):
        """
        Thread that continuously reads from the RS-485 serial port,
        processes incoming packets, and distributes data to queues.
        """
        super().__init__()
        self.port = settings.jobs_settings.reader.port
        self.settings = settings
        self.queues = queues
        self.shutdown_event = shutdown_event
        self.baudrate = settings.jobs_settings.reader.baudrate
        self.heartbeat_interval = 0.5  # Send pulse every 500ms
        self.last_heartbeat = 0

        # Initialize the DE/RE control pin
        # Set active_high=True (Standard for MAX485 DE pin)
        # initial_value=False (Start in Listen mode)
        try:
            self.max485_control = OutputDevice(5, active_high=True, initial_value=False)
        except BadPinFactory:
            Device.pin_factory = MockFactory()
            self.max485_control = OutputDevice(5, active_high=True, initial_value=False)

        self.channels = self.__map_channels()

    def run(self):
        try:
            with serial.Serial(self.port, self.baudrate, timeout=0.1) as ser:
                logger.info("Connected to RS-485 on %s at %d", self.port, self.baudrate)

                if not self._sendSettings(ser):
                    raise MCUNoResponse("MCU did not respond to settings update.")

                # Buffer to store incoming bytes
                buffer = bytearray()

                while not self.shutdown_event.is_set():
                    # send Heartbeat to keep Arduino streaming
                    if time.time() - self.last_heartbeat > self.heartbeat_interval:
                        self.max485_control.on()   # Switch MAX485 to Transmit
                        ser.write(b'\x01')         # Send pulse
                        ser.flush()                # Wait for bits to leave the UART
                        self.max485_control.off()  # Switch back to Listen immediately
                        self.last_heartbeat = time.time()

                    # read available data
                    if ser.in_waiting > 0:
                        ser.rts = True
                        buffer.extend(ser.read(ser.in_waiting))

                    # process buffer for packets
                    while len(buffer) >= Sample.PACKET_SIZE:
                        # Look for headers 0xAA 0xBB
                        if buffer[0] == 0xAA and buffer[1] == 0xBB:
                            packet_data = buffer[:Sample.PACKET_SIZE]

                            sample, checksum = Sample.from_bytes(packet_data)
                            if checksum:
                                self._process_packet(sample)
                                del buffer[:Sample.PACKET_SIZE] # Remove processed packet
                            else:
                                logger.warning("Checksum failed, shifting buffer")
                                del buffer[0] # Slide window to find next header
                        else:
                            # Not a header, discard byte and keep looking
                            del buffer[0]

        except Exception:
            logger.exception("RS485 Reader exception")
        finally:
            logger.info("RS485 Reader stopped.")
            self.shutdown_event.set()

    def _process_packet(self, data: Sample):
        timestamp = time.time()
        packet = data.to_dict(timestamp, self.channels)

        for q in self.queues:
            # Replicating your original tuple format
            q.put(packet)

    def __map_channels(self):
        return {
            i.adc_channel: i
            for i in self.settings.channels
        }

    def _sendSettings(self, ser: serial.Serial):
        time.sleep(2)   # Wait to arduino to reboot
        sent_bytes = self.settings.mcu.to_bytes  # This should be your 6-byte packet

        logger.info("Sending settings to MCU: %s", sent_bytes.hex(' '))

        # Transmit
        self.max485_control.on()   # Switch MAX485 to Transmit
        ser.write(sent_bytes)
        ser.flush()                # Block until UART buffer is physically empty
        self.max485_control.off()  # Switch back to Listen IMMEDIATELY

        # Wait for Echo/Response
        logger.info("Waiting for MCU confirmation...")

        # We look for the headers (0xCC 0xDD) in the response to ensure alignment
        response = b""
        start_time = time.time()

        while (time.time() - start_time) < 10:
            if ser.in_waiting >= MCUSettingsFrame.PACKET_SIZE:
                # Check for header alignment
                potential_header = ser.read(1)
                if potential_header == b'\xcc':
                    next_byte = ser.read(1)
                    if next_byte == b'\xdd':
                        # We found the start! Read the remaining bytes (MCUSettingsFrame.PACKET_SIZE - 2)
                        remaining = ser.read(MCUSettingsFrame.PACKET_SIZE - 2)
                        response = potential_header + next_byte + remaining
                        break
                # If not header, continue loop to effectively "drain" garbage bytes

        # Verify
        if not response:
            logger.error("MCU failed to respond (Timeout)")
            return False

        if response == sent_bytes:
            logger.info("MCU settings verified successfully!")
            return True
        else:
            logger.error("MCU verification failed!")
            logger.error("Sent:     %s", sent_bytes.hex())
            logger.error("Received: %s", response.hex())
            return False
