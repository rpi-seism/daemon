import time
from logging import getLogger
from multiprocessing import Event, Process

import serial
import zmq
from rpi_seism_common.settings import Settings

from src.exception.mcu_no_response import MCUNoResponse
from src.structs.mcu_settings import MCUSettingsFrame
from src.structs.sample import Sample
from src.utils.soh_tracker import SOHTracker

logger = getLogger(__name__)


_GPS_WARN_INTERVAL = 30.0  # Minimum seconds between GPS lock warnings when GPS is not locked. This prevents log spam during long GPS outages.


class Reader(Process):
    """
    Process that continuously reads from the RS-422 serial port,
    processes incoming packets, and distributes data to queues.
    """

    def __init__(
        self,
        settings: Settings,
        shutdown_event: Event = None,
        zmq_endpoint: str = "ipc:///tmp/seismic_data.ipc",
    ):
        super().__init__(name="ReaderProcess")
        self.port = settings.jobs_settings.reader.port
        self.settings = settings
        self.zmq_endpoint = zmq_endpoint
        self.shutdown_event = shutdown_event
        self.soh_tracker = SOHTracker()

        self.queue_len = (
            self.settings.mcu.sampling_rate * len(self.settings.channels) * 60
        ) * 5  # 5 minutes of data at 100 Hz for 3 channels

        self.baudrate = settings.jobs_settings.reader.baudrate
        self.heartbeat_interval = 0.5  # Send pulse every 500ms
        self.last_heartbeat = 0
        self.last_soh_update = 0

        self._gps_was_locked = False
        self._no_lock_count = 0
        self._locked_count = 0
        self._last_gps_warn_ts = 0

        self.channels = self.__map_channels()

    def run(self):
        # Initialize ZeroMQ
        context = zmq.Context()
        self.pub_socket = context.socket(zmq.PUB)
        self.pub_socket.set(zmq.SNDHWM, self.queue_len)
        self.pub_socket.bind(self.zmq_endpoint)

        try:
            with serial.Serial(self.port, self.baudrate, timeout=0.1) as ser:
                logger.info("Connected to RS-422 on %s at %d", self.port, self.baudrate)

                if not self._sendSettings(ser):
                    raise MCUNoResponse("MCU did not respond to settings update.")

                # Buffer to store incoming bytes
                buffer = bytearray()

                while not self.shutdown_event.is_set():
                    # send Heartbeat to keep Arduino streaming
                    if time.time() - self.last_heartbeat > self.heartbeat_interval:
                        ser.write(b"\x01")  # Send pulse
                        ser.flush()  # Wait for bits to leave the UART
                        self.last_heartbeat = time.time()

                    # read available data
                    if ser.in_waiting > 0:
                        buffer.extend(ser.read(ser.in_waiting))

                    # process buffer for packets
                    while len(buffer) >= Sample.PACKET_SIZE:
                        # Look for headers 0xAA 0xBB
                        if buffer[0] == 0xAA and buffer[1] == 0xBB:
                            packet_data = buffer[: Sample.PACKET_SIZE]

                            sample, checksum = Sample.from_bytes(packet_data)
                            if checksum:
                                self._process_packet(sample)
                                self.soh_tracker.record_success()
                                del buffer[
                                    : Sample.PACKET_SIZE
                                ]  # Remove processed packet
                            else:
                                logger.warning("Checksum failed, shifting buffer")
                                self.soh_tracker.record_checksum_error()
                                self.soh_tracker.record_dropped_bytes(1)
                                del buffer[0]  # Slide window to find next header
                        else:
                            # Not a header, discard byte and keep looking
                            self.soh_tracker.record_dropped_bytes(1)
                            del buffer[0]

                    if time.time() - self.last_soh_update > 5.0:
                        soh_stats = self.soh_tracker.get_snapshot()
                        # Send on a specific ZMQ topic or a different socket
                        self.pub_socket.send_json({"type": "SOH", "data": soh_stats})
                        self.last_soh_update = time.time()

        except Exception:
            logger.exception("RS-422 Reader exception")
        finally:
            self.soh_tracker.set_disconnected()
            logger.info("RS-422 Reader stopped.")
            self.shutdown_event.set()
            self.pub_socket.close()
            context.term()

    def _process_packet(self, data: Sample):
        timestamp = data.unix_timestamp

        if data.gps_locked:
            if not self._gps_was_locked:
                logger.info(
                    "GPS lock acquired. Switching to GPS-disciplined timestamps. "
                    "(%d packets used time.time() fallback during cold-start)",
                    self._no_lock_count,
                )
                self._gps_was_locked = True
                self._no_lock_count  = 0
            self._locked_count += 1
        else:
            self._no_lock_count += 1
            now = time.time()
            if now - self._last_gps_warn_ts > _GPS_WARN_INTERVAL:
                logger.warning(
                    "GPS not locked — using time.time() fallback for sample timestamp "
                    "(unix_sec=0 in packet). Count since last lock: %d",
                    self._no_lock_count,
                )
                self._last_gps_warn_ts = now
            if self._gps_was_locked:
                logger.warning("GPS lock LOST — reverting to time.time() fallback.")
                self._gps_was_locked = False

        packet = data.to_dict(timestamp, self.channels)

        self.pub_socket.send_json(packet)

    def __map_channels(self):
        return {i.adc_channel: i for i in self.settings.channels}

    def _sendSettings(self, ser: serial.Serial):
        time.sleep(2)  # Wait to arduino to reboot
        sent_bytes = MCUSettingsFrame.from_settings(
            self.settings
        ).to_bytes()  # This should be your 6-byte packet

        logger.info("Sending settings to MCU: %s", sent_bytes.hex(" "))

        # Transmit
        ser.write(sent_bytes)
        ser.flush()  # Block until UART buffer is physically empty

        # Wait for Echo/Response
        logger.info("Waiting for MCU confirmation...")

        # We look for the headers (0xCC 0xDD) in the response to ensure alignment
        response = b""
        start_time = time.time()

        while (time.time() - start_time) < 10:
            if ser.in_waiting >= MCUSettingsFrame.PACKET_SIZE:
                # Check for header alignment
                potential_header = ser.read(1)
                if potential_header == b"\xcc":
                    next_byte = ser.read(1)
                    if next_byte == b"\xdd":
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
