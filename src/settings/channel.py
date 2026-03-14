from .enums import ChannelOrientation

from pydantic import BaseModel


class Channel(BaseModel):
    """
    Pydantic model for a seismic data channel. This class defines the structure
    of a channel configuration, including its name, associated ADC channel, and orientation.
    It is used within the Settings model to represent individual channels in the configuration.
    """
    name: str
    adc_channel: int
    orientation: ChannelOrientation
    sensitivity: float = 28.8   # V·s/m (= V per m/s)
