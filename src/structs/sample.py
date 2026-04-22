import time
from typing import Dict
from dataclasses import dataclass
import struct

from binascii import crc32

from rpi_seism_common.settings.channel import Channel


@dataclass
class Sample:
    header_1: int
    header_2: int
    ch0: int
    ch1: int
    ch2: int
    unix_sec: int    # GPS UTC seconds; 0 = no fix
    unix_usec: int   # microseconds within that second. 0 = no pps lock
    crc: int

    # The format string for struct (little-endian, unsigned chars and ints)
    PACKET_FORMAT = "<BBiiiIII"
    PACKET_SIZE = struct.calcsize(PACKET_FORMAT)

    @property
    def gps_locked(self) -> bool:
        """True when the MCU had a valid GPS fix when this packet was built."""
        return self.unix_sec > 0 and self.unix_usec > 0

    @property
    def unix_timestamp(self) -> float:
        """
        Best-available Unix timestamp as a float.
 
        When the MCU has a GPS fix this is GPS-disciplined UTC with up to
        microsecond precision.  When GPS is not locked (unix_sec == 0) time.time() is returned, which is the best we can do without GPS.
        Note that time.time() is not guaranteed to be monotonic,
        so it may jump forward or backward if the system clock is adjusted. 
        However, this is a reasonable fallback when GPS data is unavailable.
        """
        if not self.gps_locked:
            return time.time()
        return self.unix_sec + self.unix_usec / 1_000_000.0

    @classmethod
    def from_bytes(cls, data: bytes):
        """
        Convert raw bytes to a Sample instance and verify checksum.
        Assumes that the data is already validated (correct length, headers, etc.).
        """
        if len(data) != cls.PACKET_SIZE:
            raise ValueError(f"Data length must be {cls.PACKET_SIZE} bytes.")

        # Unpack the binary data into respective fields
        header_1, header_2, ch1, ch2, ch3, unix_sec, unix_usec, checksum = struct.unpack(cls.PACKET_FORMAT, data)

        # Create the Sample instance
        sample = cls(header_1, header_2, ch1, ch2, ch3, unix_sec, unix_usec, checksum)

        # Verify checksum
        return sample, sample.verify_checksum(data)

    def to_bytes(self):
        """
        Convert the Sample instance to bytes for transmission or storage.
        """
        return struct.pack(self.PACKET_FORMAT, self.header_1, self.header_2, self.ch0, self.ch1, self.ch2, self.unix_sec, self.unix_usec, self.crc)

    def verify_checksum(self, data: bytes) -> bool:
        """
        Verify that the transmitted CRC matches the CRC32 of the data payload.
        The payload is everything EXCEPT the last 4 bytes (the CRC itself).
        """
        if len(data) != self.PACKET_SIZE:
            return False

        # Extract the payload (Headers + Channels = first 14 bytes)
        payload = data[:-4]

        # Extract the transmitted CRC from the last 4 bytes
        transmitted_crc = struct.unpack("<I", data[-4:])[0]

        # Calculate CRC32 of the payload
        calculated_crc = crc32(payload) & 0xFFFFFFFF

        return calculated_crc == transmitted_crc

    def to_dict(self, timestamp: int, channels: Dict[int, Channel]):
        return {
            "type": "packet",
            "timestamp": timestamp,
            "measurements": [
                {"channel": channels.get(0), "value": self.ch0},
                {"channel": channels.get(1), "value": self.ch1},
                {"channel": channels.get(2), "value": self.ch2}
            ]
        }
