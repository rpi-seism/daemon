from threading import Thread, Event
import time
from queue import Queue, Empty
from pathlib import Path
from logging import getLogger

from obspy import read, Stream, Trace, UTCDateTime
import numpy as np
from rpi_seism_common.settings import Settings

from src.utils.writer_utils import sds_path, split_buffer_at_midnight

logger = getLogger(__name__)


class MSeedWriter(Thread):
    """
    Thread that buffers incoming seismic data packets and writes them to
    MiniSEED files following the SeisComp Data Structure (SDS) convention.

    Files are written to:
        OUTPUT_DIR/YEAR/NET/STA/CHAN.D/NET.STA.LOC.CHAN.D.YEAR.DAY

    Each write interval flushes the buffer and appends to the current day
    file(s). If the buffer spans midnight, it is split and written to the
    correct day files automatically.

    Earthquake events trigger an early flush after 5 minutes so that the
    event waveform is persisted quickly, then the regular schedule resumes.
    """

    def __init__(
        self,
        settings: Settings,
        data_queue: Queue,
        output_dir: Path,
        shutdown_event: Event,
        earthquake_event: Event
    ):
        super().__init__()
        self.settings = settings
        self.data_queue = data_queue
        self.output_dir = output_dir
        self.write_interval_sec = settings.jobs_settings.writer.write_interval_sec
        self.shutdown_event = shutdown_event
        self.earthquake_event = earthquake_event

        # { channel_name: [raw_int_value, ...] }
        self._buffer: dict[str, list] = {}
        self._start_time: float | None = None
        self._is_processing_event = False

    def run(self):
        next_write_time = time.time() + self.write_interval_sec

        while not self.shutdown_event.is_set():
            now = time.time()

            try:
                while True:
                    packet = self.data_queue.get_nowait()
                    ts = packet["timestamp"]

                    if not self._buffer:
                        self._start_time = ts

                    for item in packet["measurements"]:
                        ch_name = item["channel"].name
                        self._buffer.setdefault(ch_name, []).append(item["value"])

                    self.data_queue.task_done()
            except Empty:
                pass

            # Earthquake early-flush trigger
            if self.earthquake_event.is_set() and not self._is_processing_event:
                next_write_time = now + 300  # flush in 5 minutes
                self._is_processing_event = True
                logger.warning("Earthquake detected — flushing to disk in 5 minutes.")

            # Scheduled
            if now >= next_write_time:
                self._flush()
                next_write_time = now + self.write_interval_sec
                self._is_processing_event = False

            time.sleep(0.01)

        # Final flush on shutdown
        self._flush()

    def _flush(self):
        """
        Write buffered samples to SDS day files and reset the buffer.
        Handles midnight splits transparently.
        """
        if not self._buffer or self._start_time is None:
            return

        logger.info(
            "Flushing %d channel(s) to SDS archive%s...",
            len(self._buffer),
            " [EARTHQUAKE]" if self._is_processing_event else "",
        )

        start = UTCDateTime(self._start_time)
        sampling_rate = self.settings.mcu.sampling_rate
        network = self.settings.station.network
        station = self.settings.station.station
        location_code = self.settings.station.location_code

        for ch_name, values in self._buffer.items():
            if not values:
                continue

            # Split at midnight so each slice lands in the correct day file
            slices = split_buffer_at_midnight(values, start, sampling_rate)

            for slice_start, slice_values in slices:
                raw = np.array(slice_values, dtype=np.int32)

                trace = Trace(data=raw)
                trace.stats.network      = network
                trace.stats.station      = station
                trace.stats.location     = location_code
                trace.stats.channel      = ch_name
                trace.stats.starttime    = slice_start
                trace.stats.sampling_rate = sampling_rate

                path = sds_path(self.output_dir, network, station, location_code, ch_name, slice_start)
                path.parent.mkdir(parents=True, exist_ok=True)

                stream = Stream([trace])

                self._write_trace(path, stream)

        self._buffer.clear()
        self._start_time = None

    def _write_trace(self, path: Path, new_stream: Stream):
        if path.exists():
            existing = read(str(path))
            combined = existing + new_stream
            combined.merge(method=1, fill_value=0)
            combined.write(str(path), format="MSEED", reclen=512)
            logger.debug("Merged %d existing and %d new samples into %s",
                         sum(len(tr.data) for tr in existing),
                         sum(len(tr.data) for tr in new_stream),
                         path.name)
        else:
            new_stream.write(str(path), format="MSEED", reclen=512)
            logger.info("Created %s", path.name)
