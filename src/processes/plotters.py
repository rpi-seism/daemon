import logging
import time
from multiprocessing import Event, Process, Queue
from pathlib import Path

from rpi_seism_common.settings import Settings

logger = logging.getLogger(__name__)


class Plotters(Process):
    def __init__(self, settings: Settings, plot_queue: Queue, shutdown_event: Event):
        super().__init__(name="PlottersProcess")
        self.settings = settings.jobs_settings.dayplot
        self.plot_queue = plot_queue
        self.shutdown_event = shutdown_event

        self.last_shutdown_event = 0

    def run(self):
        """
        The Heavy Lifter.
        Imports are local to keep the main process and other processes light.
        """
        if not self.settings.enabled:
            return

        import matplotlib

        matplotlib.use("Agg")

        from queue import Empty
        # Internal import to avoid memory bloat in other processes

        logger.info("Starting Plotters Process (DayPlotWorker)")

        shutdown_triggered_time = None

        # We want to allow the plotter to continue processing any remaining tasks for a short time after shutdown is triggered
        while True:
            try:
                if self.shutdown_event.is_set():
                    if shutdown_triggered_time is None:
                        shutdown_triggered_time = time.time()
                        logger.info(
                            "Shutdown signaled. Draining queue for up to 10 seconds."
                        )

                    # Stop if 10 seconds have passed since shutdown was triggered
                    if (
                        time.time() - shutdown_triggered_time
                        > self.settings.shutdown_timeout
                    ):
                        logger.info("Grace period expired. Force closing plotter.")
                        break
                # We use a timeout so we can check the shutdown_event periodically
                task = self.plot_queue.get(timeout=1.0)

                if task is None:
                    logger.debug("Received None, stopping process")
                    break

                # ADD THIS CHECK:
                if not isinstance(task, dict):
                    logger.warning(
                        f"Plotter received invalid task type: {type(task)} value: {task}"
                    )
                    continue

                self._generate_dayplot(task["mseed_path"], task["plot_path"])

            except Empty:
                continue
            except Exception:
                logger.exception("Error in Plotters worker loop")

        logger.info("Plotters process stopped.")

    def _generate_dayplot(self, data_path, plot_path):
        """
        Actual plotting logic using Obspy.
        """
        from obspy import read

        try:
            st = read(str(data_path))
            st.detrend("linear")
            st.taper(max_percentage=0.05)
            st.filter(
                "bandpass",
                freqmin=self.settings.low_cutoff,
                freqmax=self.settings.high_cutoff,
            )

            plot_filename = Path(plot_path).with_suffix(".png")
            tr = st[0]

            st.plot(
                type="dayplot",
                color=["black", "red", "blue", "green"],
                title=f"Helicorder: {tr.id} | {tr.stats.starttime.strftime('%Y-%j')}",
                size=(1600, 1200),
                dpi=200,
                outfile=str(plot_filename),
            )
            logger.info(f"Dayplot updated: {plot_filename.name}")
        except Exception as e:
            logger.error(f"Failed to generate plot for {data_path}: {e}")
