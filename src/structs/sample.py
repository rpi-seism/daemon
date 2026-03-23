from typing import Dict
from dataclasses import dataclass
import struct

from rpi_seism_common.settings.channel import Channel


@dataclass
class Sample:
    header_1: int
    header_2: int
    ch0: int
    ch1: int
    ch2: int
    checksum: int

    # The format string for struct (little-endian, unsigned chars and ints)
    PACKET_FORMAT = "<BBiiiB"
    PACKET_SIZE = struct.calcsize(PACKET_FORMAT)

    @classmethod
    def from_bytes(cls, data: bytes):
        """
        Convert raw bytes to a Sample instance and verify checksum.
        Assumes that the data is already validated (correct length, headers, etc.).
        """
        if len(data) != cls.PACKET_SIZE:
            raise ValueError(f"Data length must be {cls.PACKET_SIZE} bytes.")

        # Unpack the binary data into respective fields
        header_1, header_2, ch1, ch2, ch3, checksum = struct.unpack(cls.PACKET_FORMAT, data)

        # Create the Sample instance
        sample = cls(header_1, header_2, ch1, ch2, ch3, checksum)

        # Verify checksum
        return sample, sample.verify_checksum(data)

    def to_bytes(self):
        """
        Convert the Sample instance to bytes for transmission or storage.
        """
        return struct.pack(self.PACKET_FORMAT, self.header_1, self.header_2, self.ch0, self.ch1, self.ch2, self.checksum)

    def verify_checksum(self, data: bytes):
        """
        Verify that the checksum matches based on the XOR of all fields except the checksum itself.
        """
        calculated = 0
        for b in data[:-1]:
            calculated ^= b
        return calculated == data[-1]

    def to_dict(self, timestamp: int, channels: Dict[int, Channel]):
        return {
            "timestamp": timestamp,
            "measurements": [
                {"channel": channels.get(0), "value": self.ch0},
                {"channel": channels.get(1), "value": self.ch1},
                {"channel": channels.get(2), "value": self.ch2}
            ]
        }
