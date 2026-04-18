from multiprocessing import Process, Event
from collections import deque
from logging import getLogger
import asyncio

import numpy as np
import websockets
import zmq
import zmq.asyncio
from obspy import UTCDateTime, Trace

from rpi_seism_common.settings import Settings
from rpi_seism_common.websocket_message import WebsocketMessage

from src.ws_messages.sample.sample import Sample
from src.ws_messages.sample.sample_payload import SamplePayload
from src.ws_messages.state_of_health.state_of_health import StateOfHealth
from src.ws_messages.state_of_health.state_of_health_payload import StateOfHealthPayload


logger = getLogger(__name__)


class WebSocketSender(Process):
    """Thread that serves a WebSocket endpoint to broadcast decimated seismic data
    in real-time to connected clients. It maintains a sliding window buffer for each channel,
    applies decimation, and sends downsampled data every second.
    """
    def __init__(
        self,
        settings: Settings,
        shutdown_event: Event,
        earthquake_event: Event,
        zmq_endpoint: str = "ipc:///tmp/seismic_data.ipc",
        host: str = "0.0.0.0",
        port: int = 8765
    ):
        super().__init__(daemon=True)
        self.shutdown_event = shutdown_event
        self.earthquake_event = earthquake_event
        self.zmq_endpoint = zmq_endpoint
        self.host = host
        self.port = port
        self.settings = settings

        self._clients = set()

        # Sliding Window Config
        # window_size: 5s buffer for filter stability
        # step_size: 1s update interval
        self.window_size = int(self.settings.mcu.sampling_rate * 5)
        self.step_size = int(self.settings.mcu.sampling_rate)

        # Per-channel state: { "EHZ": {"data": deque, "time": deque, "counter": 0}, ... }
        self.channels_state = {}
        self.latest_soh_data = {}

        # SOH broadcast interval (seconds)
        self.soh_interval = 5.0
        self.last_soh_broadcast = 0.0

    def run(self):
        asyncio.run(self._main_loop())

    async def _main_loop(self):
        self.ctx = zmq.asyncio.Context()
        self.sub_socket = self.ctx.socket(zmq.SUB)
        self.sub_socket.connect(self.zmq_endpoint)
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "") # Subscribe to all

        async with websockets.serve(self._handle_connection, self.host, self.port):
            logger.info("WebSocket Server started on ws://%s:%d", self.host, self.port)
            await self._producer_loop()

    async def _handle_connection(self, websocket):
        self._clients.add(websocket)
        try:
            await websocket.wait_closed()
        finally:
            self._clients.discard(websocket)

    async def _producer_loop(self):
        while not self.shutdown_event.is_set():
            try:
                # Expecting: {"timestamp": float, "measurements": [{"channel": obj, "value": int}, ...]}
                packet = await asyncio.wait_for(
                    self.sub_socket.recv_pyobj(), 
                    timeout=1.0
                )

                # Filter for packets
                if packet.get("type") != "packet":
                    # If this is an SOH packet, update your local tracker
                    if packet.get("type") == "SOH":
                        self.latest_soh_data = packet["data"]
                    continue

                ts = packet["timestamp"]

                # update each channel's buffer
                for item in packet["measurements"]:
                    ch_name = item["channel"].name
                    val = item["value"]

                    if ch_name not in self.channels_state:
                        self.channels_state[ch_name] = {
                            "data": deque(maxlen=self.window_size),
                            "time": deque(maxlen=self.window_size),
                            "counter": 0
                        }

                    state = self.channels_state[ch_name]
                    state["data"].append(float(val))
                    state["time"].append(ts)
                    state["counter"] += 1

                    # process every STEP_SIZE samples for THIS specific channel
                    if (len(state["data"]) == self.window_size and
                        state["counter"] % self.step_size == 0):
                        await self._process_and_broadcast(ch_name)

                # Periodic SOH Broadcast to Web Clients
                now = asyncio.get_event_loop().time()
                if now - self.last_soh_broadcast >= self.soh_interval:
                    await self._broadcast_soh()
                    self.last_soh_broadcast = now

            except asyncio.TimeoutError:
                continue
            except Exception:
                logger.exception("Error in WebSocket producer loop")
        
        self.sub_socket.close()
        self.ctx.term()

    async def _process_and_broadcast(self, channel_name):
        """Perform decimation and broadcast for a specific channel."""
        # If no WebSocket clients are connected, don't waste CPU on obspy
        if not self._clients:
            return

        state = self.channels_state[channel_name]

        # Create Trace from current buffer
        data_array = np.array(state["data"])
        tr = Trace(data=data_array)
        tr.stats.sampling_rate = self.settings.mcu.sampling_rate
        tr.stats.starttime = UTCDateTime(state["time"][0])

        # Decimate (Anti-Alias filter applied)
        tr_decimated = tr.copy()
        try:
            tr_decimated.filter("bandpass",freqmin=0.2, freqmax=10.0)
            # Note: decimation_factor must be e.g., 2, 4, 5, 8, 10
            tr_decimated.decimate(self.settings.decimation_factor, no_filter=False)
        except Exception as e:
            logger.error("Decimation failed for %s: %s", channel_name, e)
            return

        # Extract the new batch of downsampled samples
        new_samples_count = int(self.step_size / self.settings.decimation_factor)
        downsampled_values = tr_decimated.data[-new_samples_count:]

        # Construct and send the message
        message = SamplePayload(
            channel=channel_name,
            timestamp=tr_decimated.stats.endtime.isoformat() + "Z",
            fs=tr_decimated.stats.sampling_rate,
            data=downsampled_values.tolist()
        )

        await self._broadcast(Sample(payload=message))

    async def _broadcast_soh(self):
        """Broadcast current State of Health metrics to all connected clients."""
        # If no WebSocket clients are connected, don't waste CPU
        if not self._clients:
            return

        if not self.latest_soh_data:
            return

        snapshot = self.latest_soh_data

        payload = StateOfHealthPayload(
            link_quality=snapshot["link_quality"],
            bytes_dropped=snapshot["bytes_dropped"],
            checksum_errors=snapshot["checksum_errors"],
            last_seen=snapshot["last_seen"],
            connected=snapshot["connected"]
        )

        message = StateOfHealth(payload=payload)
        await self._broadcast(message)

    async def _broadcast(self, message: WebsocketMessage):
        if not self._clients:
            return

        payload = message.to_json

        dead_clients = set()
        send_tasks = [self._safe_send(ws, payload, dead_clients) for ws in self._clients]
        if send_tasks:
            await asyncio.gather(*send_tasks)

        if dead_clients:
            self._clients.difference_update(dead_clients)

    async def _safe_send(self, websocket, message, dead_clients):
        try:
            await websocket.send(message)
        except Exception:
            dead_clients.add(websocket)
