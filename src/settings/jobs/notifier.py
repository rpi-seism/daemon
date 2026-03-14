from pydantic import BaseModel


class Notifier(BaseModel):
    url: str
    enabled: bool = True
