from collections import deque
from multiprocessing import Process, Event
from logging import getLogger

import numpy as np
import zmq

# ObsPy's recursive STA/LTA is faster and better for continuous data
from obspy.signal.trigger import recursive_sta_lta
from rpi_seism_common.settings import Settings


logger = getLogger(__name__)


class TriggerProcessor(Process):
    """
    Thread that processes incoming seismic data packets using ObsPy's recursive STA/LTA.
    Uses a rolling buffer to maintain the state required for the algorithm.
    """
    def __init__(
        self,
        settings: Settings,
        shutdown_event: Event,
        earthquake_event: Event,
        zmq_endpoint: str = "ipc:///tmp/seismic_data.ipc",
    ):
        super().__init__()
        self.earthquake_event = earthquake_event
        self.shutdown_event = shutdown_event
        self.zmq_endpoint = zmq_endpoint

        # Configuration from settings
        self.sampling_rate = settings.mcu.sampling_rate
        self.trigger_channel = settings.jobs_settings.trigger.trigger_channel

        # STA/LTA Window lengths in seconds
        self.sta_sec = settings.jobs_settings.trigger.sta_sec
        self.lta_sec = settings.jobs_settings.trigger.lta_sec

        # Trigger thresholds
        self.thr_on = settings.jobs_settings.trigger.thr_on   # Ratio to trigger
        self.thr_off = settings.jobs_settings.trigger.thr_off  # Ratio to clear trigger

        # Convert seconds to sample counts for ObsPy
        self.nsta = int(self.sta_sec * self.sampling_rate)
        self.nlta = int(self.lta_sec * self.sampling_rate)

        # Buffer: We need at least nlta samples to establish a baseline.
        # We keep a slightly larger buffer (e.g., 2x LTA) to ensure stable ratios.
        self.buffer_size = self.nlta * 2
        self.data_buffer = deque(maxlen=self.buffer_size)

        self.last_trigger = False

    def run(self):
        logger.info("Trigger Processor (ObsPy Recursive STA/LTA) started.")

        context = zmq.Context()
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(self.zmq_endpoint)
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "") # Receive everything

        while not self.shutdown_event.is_set():
            try:
                # Expecting: {"timestamp": float, "measurements": [{"channel": obj, "value": int}, ...]}
                packet = sub_socket.recv_pyobj()

                if packet.get("type") != "packet":
                    continue

                # Extract the value for the trigger channel
                trigger_value = next(
                    (item["value"] for item in packet["measurements"]
                     if item["channel"].name == self.trigger_channel),
                    None
                )

                if trigger_value is None:
                    self.data_queue.task_done()
                    continue

                # Add new sample to the rolling buffer
                self.data_buffer.append(float(trigger_value))

                # Process if we have enough data for the LTA window
                if len(self.data_buffer) >= self.nlta:
                    self._update_trigger_state()

                self.data_queue.task_done()

            except zmq.Again:
                # This exception is raised when RCVTIMEO is hit
                pass
            except Exception:
                logger.exception("Error in Trigger Processor loop")

        sub_socket.close()
        context.term()
        logger.info("Trigger Processor stopped.")

    def _update_trigger_state(self):
        """Calculates the characteristic function and handles event state."""
        # Convert buffer to numpy array for ObsPy processing
        data_arr = np.array(self.data_buffer)

        # ObsPy's recursive_sta_lta returns the 'Characteristic Function' (the ratios)
        cft = recursive_sta_lta(data_arr, self.nsta, self.nlta)

        # The latest ratio is the last element of the array
        current_ratio = cft[-1]

        # Handle State Changes (Edge Detection) with Dual Thresholds (Hysteresis)
        if current_ratio > self.thr_on and not self.last_trigger:
            logger.warning(f"EARTHQUAKE DETECTED: STA/LTA ratio {current_ratio:.2f} > {self.thr_on}")
            self.earthquake_event.set()
            self.last_trigger = True

        elif current_ratio < self.thr_off and self.last_trigger:
            logger.info(f"Trigger cleared: Signal ratio {current_ratio:.2f} returned below {self.thr_off}")
            self.earthquake_event.clear()
            self.last_trigger = False
