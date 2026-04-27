from datetime import datetime

from rpi_seism_common.settings import BaseModel


class Bookmark(BaseModel):
    label: str
    start: datetime
    end: datetime
    units: str
    channels: list[str]
