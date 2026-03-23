from pathlib import Path
from logging import getLogger

from obspy import UTCDateTime

logger = getLogger(__name__)


def sds_path(archive_root: Path, network: str, station: str,
              location_code: str, channel: str, t: UTCDateTime) -> Path:
    """
    Returns the SDS file path for a given channel and UTC time.

    Structure:
        ROOT/YEAR/NET/STA/CHAN.D/NET.STA.LOC.CHAN.D.YEAR.DAY

    Example:
        archive/2026/XX/RPI3/EHZ.D/XX.RPI3.00.EHZ.D.2025.069
    """
    filename = (
        f"{network}.{station}.{location_code}.{channel}"
        f".D.{t.year}.{t.julday:03d}"
    )
    return archive_root / "archive" / str(t.year) / network / station / f"{channel}.D" / filename


def split_buffer_at_midnight(
    values: list,
    start_time: UTCDateTime,
    sampling_rate: float,
) -> list[tuple[UTCDateTime, list]]:
    """
    Split a buffer of samples at UTC midnight boundaries.
    Returns a list of (start_time, samples) slices, one per calendar day.
    """
    if not values:
        return []

    slices = []
    slice_start = start_time
    slice_values = []

    seconds_per_sample = 1.0 / sampling_rate

    for i, v in enumerate(values):
        sample_time = start_time + i * seconds_per_sample

        # Detect crossing into a new UTC day
        if slice_values and sample_time.julday != slice_start.julday:
            slices.append((slice_start, slice_values))
            slice_start = sample_time
            slice_values = []

        slice_values.append(v)

    if slice_values:
        slices.append((slice_start, slice_values))

    return slices
