from pydantic import BaseModel


class Writer(BaseModel):
    """Configuration for the writer job."""
    write_interval_sec: float = 1800  # Write data every 30 minutes
