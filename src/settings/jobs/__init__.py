from pydantic import BaseModel

from .trigger import Trigger
from .notifier import Notifier
from .writer import Writer
from .reader import Reader


class JobsSettings(BaseModel):
    """Pydantic model for job-related settings."""
    trigger: Trigger
    notifiers: list[Notifier]
    writer: Writer
    reader: Reader
