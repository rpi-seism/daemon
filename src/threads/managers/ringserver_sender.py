from multiprocessing import Event
from threading import Thread

import zmq
import time
from logging import getLogger
from io import BytesIO

from obspy import Trace, UTCDateTime
import numpy as np
from rpi_seism_common.settings import Settings

from datalink_client import DataLink, DataLinkError

logger = getLogger(__name__)


class RingServerSender(Thread):
    def __init__(
            self,
            settings: Settings,
            shutdown_event: Event,
            zmq_endpoint: str = "ipc:///tmp/seismic_data.ipc"
        ):
        super().__init__(daemon=True)
        self.settings = settings
        self.shutdown_event = shutdown_event
        self.zmq_endpoint = zmq_endpoint

        self.ring_server_settings = self.settings.jobs_settings.ring_server
        self.write_interval_sec = self.ring_server_settings.write_interval_sec

        self._buffer: dict[str, list] = {}
        self._start_time: float | None = None

        self.client = None

    def run(self):
        logger.info("RingServer sender started")
        next_write_time = time.time() + self.write_interval_sec

        context = zmq.Context()
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(self.zmq_endpoint)
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        sub_socket.setsockopt(zmq.RCVTIMEO, 100)

        while not self.shutdown_event.is_set():
            now = time.time()

            # Ensure Connection
            if self.client is None or not self.client.is_connected:
                self._attempt_connection()

            # Consume Queue
            try:
                packet = sub_socket.recv_json()

                if packet.get("type") != "packet":
                    continue

                if not self._buffer:
                    self._start_time = packet["timestamp"]

                for item in packet["measurements"]:
                    ch_name = item["channel"].name
                    self._buffer.setdefault(ch_name, []).append(item["value"])
            except zmq.Again:
                # No more data in the ZMQ socket for now
                pass

            # Periodic flush
            if now >= next_write_time:
                self._flush()
                next_write_time = now + self.write_interval_sec

            time.sleep(0.01)

        self._flush()
        if self.client:
            self.client.close()

        sub_socket.close()
        context.term()

    def _attempt_connection(self):
        try:
            # Create client (host, port)
            self.client = DataLink(self.ring_server_settings.host, self.ring_server_settings.port)
            self.client.connect()

            # Identify is crucial for Ringserver to accept the stream
            server_id = self.client.identify(clientid=f"{self.settings.station.station}_{self.settings.station.station}")
            logger.info("Connected to Ringserver: %s", server_id)
        except Exception as e:
            logger.error("DataLink connection failed: %s", e)
            self.client = None

    def _flush(self):
        if not self.client or not self.client.is_connected:
            return
        if not self._buffer or self._start_time is None:
            return

        # Explicitly define metadata to avoid NameError
        net = self.settings.station.network
        sta = self.settings.station.station
        loc = self.settings.station.location_code
        rate = self.settings.mcu.sampling_rate
        start_utc = UTCDateTime(self._start_time)

        try:
            # Use the batch context manager from your source code
            # This sends all channels in one network burst
            with self.client.batch():
                for ch, values in self._buffer.items():
                    if not values: continue

                    # Create MiniSEED
                    trace = Trace(data=np.array(values, dtype=np.int32))
                    trace.stats.update({
                        'network': net, 'station': sta, 'location': loc, 
                        'channel': ch, 'starttime': start_utc, 'sampling_rate': rate
                    })

                    buf = BytesIO()
                    trace.write(buf, format="MSEED", reclen=512)
                    mseed_data = buf.getvalue()

                    # DataLink 1.1 uses microseconds for timestamps
                    # The library's write() actually takes ints/strings
                    # but converts them if needed. 
                    # Based on your source, we pass them as microseconds:
                    start_us = int(trace.stats.starttime.timestamp * 1_000_000)
                    end_us = int(trace.stats.endtime.timestamp * 1_000_000)

                    stream_id = f"{net}_{sta}_{loc}_{ch}/MSEED"

                    self.client.write(stream_id, start_us, end_us, mseed_data)

            logger.info(f"Flushed {len(self._buffer)} channels to Ringserver")

        except (DataLinkError, OSError) as e:
            logger.error(f"Flush failed: {e}")
            self.client.close()
            self.client = None
        finally:
            self._buffer.clear()
            self._start_time = None
