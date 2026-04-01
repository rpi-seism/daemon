import hashlib
import json
import logging
from pathlib import Path

from obspy import UTCDateTime
from obspy.core.inventory import (
    Channel,
    Inventory,
    InstrumentSensitivity,
    Network,
    PolesZerosResponseStage,
    Response,
    ResponseStage,
    Site,
    Station,
)
from obspy.signal.invsim import corn_freq_2_paz
from rpi_seism_common.settings import Settings

from src.exception.station_xml_epoch_error import StationXMLEpochError


logger = logging.getLogger(__name__)

# GD-4.5 physical parameters
# Adjust _GD45_DAMPING if you have an external load resistor on the geophone
_GD45_NATURAL_FREQ = 4.5  # Hz
_GD45_DAMPING = 0.6

_ORIENTATION_MAP = {
    "vertical": {"azimuth": 0.0,  "dip": -90.0},
    "north":    {"azimuth": 0.0,  "dip":   0.0},
    "east":     {"azimuth": 90.0, "dip":   0.0},
}


def _fingerprint(settings: Settings) -> str:
    """
    Stable hash of every setting that affects the StationXML content.
    If this hash changes, the file must be regenerated.
    """
    relevant = {
        "network":  settings.station.network,
        "station":  settings.station.station,
        "sampling_rate": settings.mcu.sampling_rate,
        "adc_gain": settings.mcu.adc_gain,
        "vref":     settings.mcu.vref,
        "channels": [
            {
                "name":        ch.name,
                "orientation": ch.orientation,
                "sensitivity": ch.sensitivity,
                "analog_gain": ch.analog_gain
            }
            for ch in sorted(settings.channels, key=lambda c: c.name)
        ],
    }
    blob = json.dumps(relevant, sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()


def _build_channel_response(settings: Settings, sensitivity: float, analog_gain: float) -> Response:
    counts_per_volt = (settings.mcu.adc_gain * 2**23) / settings.mcu.vref
    total_sensitivity = sensitivity * analog_gain * counts_per_volt

    # Compute PAZ analytically from f0 and damping via ObsPy.
    paz = corn_freq_2_paz(fc=_GD45_NATURAL_FREQ, damp=_GD45_DAMPING)

    paz_stage = PolesZerosResponseStage(
        stage_sequence_number=1,
        stage_gain=sensitivity,
        stage_gain_frequency=_GD45_NATURAL_FREQ,
        input_units="M/S",
        output_units="V",
        pz_transfer_function_type="LAPLACE (RADIANS/SECOND)",
        normalization_frequency=_GD45_NATURAL_FREQ,
        normalization_factor=paz['gain'],
        zeros=paz['zeros'],
        poles=paz['poles'],
    )

    # NEW — instrumentation amplifier stage
    amp_stage = ResponseStage(
        stage_sequence_number=2,
        stage_gain=analog_gain,
        stage_gain_frequency=0.0,
        input_units="V",
        output_units="V",
    )

    adc_stage = ResponseStage(
        stage_sequence_number=3,    # ← was 2
        stage_gain=counts_per_volt,
        stage_gain_frequency=0.0,
        input_units="V",
        output_units="COUNTS",
    )

    return Response(
        instrument_sensitivity=InstrumentSensitivity(
            value=total_sensitivity,
            frequency=_GD45_NATURAL_FREQ,
            input_units="M/S",
            output_units="COUNTS",
            input_units_description="Velocity in meters per second",
            output_units_description="Digital counts",
        ),
        response_stages=[paz_stage, amp_stage, adc_stage],  # ← 3 stages
    )


def _build_inventory(settings: Settings) -> Inventory:
    channels = []

    for ch in settings.channels:
        orientation = _ORIENTATION_MAP.get(ch.orientation.lower())
        if orientation is None:
            raise ValueError(
                f"Unknown orientation '{ch.orientation}' for channel '{ch.name}'. "
                f"Must be one of: {list(_ORIENTATION_MAP)}"
            )

        response = _build_channel_response(settings, ch.sensitivity, ch.analog_gain)

        channels.append(
            Channel(
                code=ch.name,
                location_code="00",
                latitude=settings.station.latitude,
                longitude=settings.station.longitude,
                elevation=settings.station.elevation,
                depth=0.0,
                azimuth=orientation["azimuth"],
                dip=orientation["dip"],
                sample_rate=settings.mcu.sampling_rate,
                response=response,
                start_date=UTCDateTime(settings.start_date),
            )
        )

    station = Station(
        code=settings.station.station,
        latitude=settings.station.latitude,
        longitude=settings.station.longitude,
        elevation=settings.station.elevation,
        channels=channels,
        site=Site(name=f"{settings.station.station} Station"),
        start_date=UTCDateTime(settings.start_date),
    )

    return Inventory(
        networks=[Network(code=settings.station.network, stations=[station])],
        source=settings.station.station,
    )


def _read_sidecar(fingerprint_path: Path) -> dict:
    """Read the JSON sidecar file {fingerprint, start_date}."""
    return json.loads(fingerprint_path.read_text())


def _write_sidecar(fingerprint_path: Path, fingerprint: str, start_date: str) -> None:
    """Write the JSON sidecar file."""
    fingerprint_path.write_text(
        json.dumps({"fingerprint": fingerprint, "start_date": start_date}, indent=2)
    )


def _close_and_append_epochs(
    settings: Settings,
    output_path: Path,
    new_start: UTCDateTime,
) -> None:
    """
    Read the existing station.xml, close all open channel epochs by setting
    their end_date to new_start, then append new channel epochs built from
    the current settings.
    """
    from obspy import read_inventory  # local import — obspy may not always be needed

    inventory = read_inventory(str(output_path))

    new_channels = {
        ch.name: ch for ch in settings.channels
    }

    for network in inventory.networks:
        for station in network.stations:
            # Close every open epoch for channels we manage
            for existing_ch in station.channels:
                if existing_ch.code in new_channels and existing_ch.end_date is None:
                    existing_ch.end_date = new_start
                    logger.info(
                        "Closed epoch for channel %s at %s",
                        existing_ch.code, new_start,
                    )

            # Append a new epoch for each channel
            for ch in settings.channels:
                orientation = _ORIENTATION_MAP.get(ch.orientation.lower())
                if orientation is None:
                    raise ValueError(
                        f"Unknown orientation '{ch.orientation}' for channel '{ch.name}'. "
                        f"Must be one of: {list(_ORIENTATION_MAP)}"
                    )

                response = _build_channel_response(settings, ch.sensitivity, ch.analog_gain)

                station.channels.append(
                    Channel(
                        code=ch.name,
                        location_code="00",
                        latitude=settings.station.latitude,
                        longitude=settings.station.longitude,
                        elevation=settings.station.elevation,
                        depth=0.0,
                        azimuth=orientation["azimuth"],
                        dip=orientation["dip"],
                        sample_rate=settings.mcu.sampling_rate,
                        response=response,
                        start_date=new_start,
                    )
                )
                logger.info("Opened new epoch for channel %s at %s", ch.name, new_start)

    inventory.write(str(output_path), format="STATIONXML")
    logger.info("station.xml updated with new epochs.")


def ensure_station_xml(settings: Settings, output_path: Path) -> Path:
    """
    Ensures a valid station.xml exists at output_path.

    Behaviour:
      - First run:
            Generates station.xml and writes a JSON sidecar (.sha256)
            storing the settings fingerprint and start_date.

      - No settings change:
            Does nothing.

      - Settings changed, start_date unchanged:
            Raises StationXMLEpochError — the caller must update start_date
            in their settings to mark the hardware change boundary before
            the application is allowed to start.

      - Settings changed, start_date changed:
            Automatically closes all open channel epochs (sets end_date) and
            appends new epochs with the new response and new start_date.
            Updates the sidecar. Safe to commit to version control afterwards.

    Returns the path to the existing or newly created station.xml.
    """
    output_path = Path(output_path)
    fingerprint_path = output_path.with_suffix(".sha256")

    current_fingerprint = _fingerprint(settings)
    current_start_date  = str(settings.start_date)

    #  First run 
    if not output_path.exists():
        logger.info("station.xml not found — generating for the first time.")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        inventory = _build_inventory(settings)
        inventory.write(str(output_path), format="STATIONXML")
        _write_sidecar(fingerprint_path, current_fingerprint, current_start_date)

        logger.info(
            "station.xml written to %s — "
            "keep this file in version control and never delete existing epochs.",
            output_path,
        )
        return output_path

    #  File exists but no sidecar (manual creation) 
    if not fingerprint_path.exists():
        _write_sidecar(fingerprint_path, current_fingerprint, current_start_date)
        logger.info(
            "station.xml found without a sidecar file. "
            "Sidecar written for future change detection. "
            "Ensure the file matches your current settings."
        )
        return output_path

    #  Compare against saved state 
    sidecar = _read_sidecar(fingerprint_path)
    saved_fingerprint = sidecar["fingerprint"]
    saved_start_date  = sidecar["start_date"]

    if saved_fingerprint == current_fingerprint:
        logger.debug("station.xml is up to date — nothing to do.")
        return output_path

    # Settings have changed — check whether start_date was also updated.
    if current_start_date == saved_start_date:
        raise StationXMLEpochError(
            "Instrument settings have changed but 'start_date' has not been updated.\n"
            "You must set a new start_date in your settings to mark the boundary "
            "between the old and new hardware configuration.\n"
            f"  Current start_date : {current_start_date}\n"
            "  Update it to the date/time of the hardware change and restart."
        )

    # start_date has changed — automatically manage the epoch transition.
    logger.info(
        "Settings changed and start_date updated (%s → %s). "
        "Closing old epochs and appending new ones.",
        saved_start_date, current_start_date,
    )

    _close_and_append_epochs(
        settings,
        output_path,
        new_start=UTCDateTime(settings.start_date),
    )
    _write_sidecar(fingerprint_path, current_fingerprint, current_start_date)

    return output_path
