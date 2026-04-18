from multiprocessing import Process, Event
from io import BytesIO
from collections import deque
from logging import getLogger
import time
from datetime import datetime

import zmq

from apprise import Apprise, NotifyFormat
from apprise.attachment.memory import AttachMemory

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd

from rpi_seism_common.settings import Settings


logger = getLogger(__name__)


class NotifierSender(Process):
    def __init__(
        self,
        settings: Settings,
        shutdown_event: Event,
        earthquake_event: Event,
        zmq_endpoint: str = "ipc:///tmp/seismic_data.ipc"
    ):
        super().__init__()
        self.settings = settings
        self.earthquake_event = earthquake_event
        self.shutdown_event = shutdown_event
        self.zmq_endpoint = zmq_endpoint

        self.notifier = Apprise()
        self.last_notification = 0

        self.points_per_window = self.settings.mcu.sampling_rate * 60
        self.total_capacity = self.points_per_window * 2
        self.buffer = deque(maxlen=self.total_capacity)

    def run(self):
        logger.info("Notifier Sender started.")
        self._initialize_notifier()

        context = zmq.Context()
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(self.zmq_endpoint)
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "") # Receive everything

        sub_socket.setsockopt(zmq.RCVTIMEO, 100) # 100ms timeout

        while not self.shutdown_event.is_set():
            try:
                try:
                    packet = sub_socket.recv_pyobj()
                    if packet.get("type") == "packet":
                        self.buffer.append(packet)
                except zmq.Again:
                    pass # Timeout reached, just check events

                # Check for trigger (with 30s cooldown)
                if self.earthquake_event.is_set() and (time.time() - self.last_notification > 30):
                    self.notifier.notify(
                        title="⚠️ Earthquake Alert",
                        body='Significant seismic activity detected!',
                        body_format=NotifyFormat.MARKDOWN
                    )
                    logger.info("Triggered! Collecting 60s post-event data...")
                    self._handle_event()
                    self.last_notification = time.time()

            except Exception:
                logger.exception("Error in Notifier loop")

        sub_socket.close()
        context.term()

    def _handle_event(self):
        """Waits for post-event data, generates graph, and sends."""
        # Record how many more samples we need to finish the 'after' window
        # (Already have 60s in buffer, need 60s more)
        received = 0

        while received < self.points_per_window and not self.shutdown_event.is_set():
            try:
                data = self.queue.get(timeout=1.0)
                self.buffer.append(data)
                received += 1
            except Exception:
                continue

        # Generate and Send
        graph_bytes = self._generate_plotly_graph()
        self._send_notification(graph_bytes)

    def _generate_plotly_graph(self) -> BytesIO:
        """Parses buffer into DataFrame and creates a multi-channel Plotly graph."""
        # Flatten the complex dict structure into a list for Pandas
        rows = []
        for packet in self.buffer:
            ts = packet['timestamp']
            for m in packet['measurements']:
                rows.append({
                    "time": datetime.fromtimestamp(ts),
                    "channel": m['channel'].name, # e.g., "Channel Z"
                    "value": m['value']
                })

        df = pd.DataFrame(rows)
        channels = df['channel'].unique()

        # Create subplots (one for each axis/channel)
        fig = make_subplots(rows=len(channels), cols=1, shared_xaxes=True, vertical_spacing=0.05)

        for i, ch in enumerate(channels, 1):
            ch_data = df[df['channel'] == ch]
            fig.add_trace(
                go.Scatter(x=ch_data['time'], y=ch_data['value'], name=ch),
                row=i, col=1
            )

        fig.update_layout(height=200*len(channels), title_text="Seismic Event Detail (120s)")

        # Return as PNG image bytes
        html = fig.to_html()

        traces_bytes = BytesIO()
        traces_bytes.write(html.encode("utf-8"))
        traces_bytes.seek(0)
        traces_bytes.name = "trace.html"

        return traces_bytes

    def _send_notification(self, image_stream):
        """Sends the notification with the attached graph."""
        # Apprise allows attaching file streams
        self.notifier.notify(
            title="⚠️ Earthquake Alert",
            body="Seismic activity exceeded threshold. See attached waveform.",
            attach=AttachMemory(
                content=image_stream.getvalue(),
                name="report.html",
                mimetype="text/html",
            ),
            body_format=NotifyFormat.MARKDOWN
        )

    def _initialize_notifier(self):
        for i in self.settings.jobs_settings.notifiers:
            if i.enabled:
                self.notifier.add(i.url)
