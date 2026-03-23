from typing import Tuple

from dataclasses import dataclass
import struct

from rpi_seism_common.settings import Settings


@dataclass
class MCUSettingsFrame:
    header_1: int
    header_2: int
    sampling_speed: int
    adc_gain: int
    adc_data_rate: int

    # The format string for struct (little-endian)
    PACKET_FORMAT = "<BBHBB"
    PACKET_SIZE = struct.calcsize(PACKET_FORMAT)

    @classmethod
    def from_bytes(cls, data: bytes) -> Tuple["MCUSettingsFrame", bool]:
        """
        Convert raw bytes to a MCUSettingsFrame instance and verify checksum.
        Assumes that the data is already validated (correct length, headers, etc.).
        """
        if len(data) != cls.PACKET_SIZE:
            raise ValueError(f"Data length must be {cls.PACKET_SIZE} bytes.")

        # Unpack the binary data into respective fields
        header_1, header_2, sampling_speed, adc_gain, adc_data_rate = struct.unpack(cls.PACKET_FORMAT, data)

        # Create the MCUSettingsFrame instance
        settings_frame = cls(header_1, header_2, sampling_speed, adc_gain, adc_data_rate)

        # Verify checksum
        return settings_frame, settings_frame.verify_checksum(data)

    @classmethod
    def from_settings(cls, settings: Settings) -> "MCUSettingsFrame":
        """
        Convert settings into an MCUSettingsFrame
        """
        return cls(
            0xCC,
            0xDD,
            settings.mcu.sampling_rate,
            settings.mcu.adc_gain,
            settings.mcu.adc_sample_rate
        )

    def to_bytes(self):
        """
        Convert the Sample instance to bytes for transmission or storage.
        """
        return struct.pack(self.PACKET_FORMAT, self.header_1, self.header_2, self.sampling_speed, self.adc_gain, self.adc_data_rate)

    def verify_checksum(self, data: bytes):
        """
        Verify that the checksum matches based on the XOR of all fields except the checksum itself.
        """
        calculated = 0
        for b in data[:-1]:
            calculated ^= b
        return calculated == data[-1]
