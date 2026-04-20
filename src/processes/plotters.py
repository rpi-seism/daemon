import logging
from multiprocessing import Event, Process, Queue
from pathlib import Path

logger = logging.getLogger(__name__)


class Plotters(Process):
    def __init__(self, plot_queue: Queue, shutdown_event: Event):
        super().__init__(name="PlottersProcess")
        self.plot_queue = plot_queue
        self.shutdown_event = shutdown_event

    def run(self):
        """
        The Heavy Lifter.
        Imports are local to keep the main process and other processes light.
        """
        import matplotlib

        matplotlib.use("Agg")

        from queue import Empty
        # Internal import to avoid memory bloat in other processes
        # from src.jobs.plotter import DayPlotWriter

        logger.info("Starting Plotters Process (DayPlotWorker)")

        while True:
            try:
                # We use a timeout so we can check the shutdown_event periodically
                task = self.plot_queue.get(timeout=1.0)

                if task is None:
                    logger.debug("Received None, stopping process")
                    break

                # ADD THIS CHECK:
                if not isinstance(task, dict):
                    logger.warning(f"Plotter received invalid task type: {type(task)} value: {task}")
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
            st.filter("bandpass", freqmin=0.2, freqmax=40)

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
