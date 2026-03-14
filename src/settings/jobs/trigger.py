from pydantic import BaseModel


class Trigger(BaseModel):
    """Configuration for the trigger processor."""
    # STA/LTA Window lengths in seconds
    sta_sec: float = 0.5
    lta_sec: float = 10.0

    # Trigger thresholds
    thr_on: float = 3.5   # Ratio to trigger
    thr_off: float = 1.5  # Ratio to clear trigger

    trigger_channel: str = "EHZ"
