from pydantic import BaseModel


class Reader(BaseModel):
    """Configuration for the reader job."""
    port: str = "/dev/ttyUSB0"  # Serial port for the MCU
    baudrate: int = 250000       # Baud rate for serial communication
