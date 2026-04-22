import logging
from multiprocessing import Event, Process

logger = logging.getLogger(__name__)


class Managers(Process):
    def __init__(
        self, settings, shutdown_event: Event, trigger_event: Event, zmq_addr: str
    ):
        super().__init__(name="ManagersProcess")
        self.settings = settings
        self.shutdown_event = shutdown_event
        self.trigger_event = trigger_event
        self.zmq_addr = zmq_addr

    def run(self):
        from src.threads.managers import (
            NotifierSender,
            RingServerSender
        )

        logger.info("Starting Managers Process (Notifier + RingServer)")

        # Initialize jobs
        jobs = []

        if any(x.enabled for x in self.settings.jobs_settings.notifiers):
            notifier_job = NotifierSender(
                self.settings, self.shutdown_event, self.trigger_event, self.zmq_addr
            )
            jobs.append(notifier_job)

        if self.settings.jobs_settings.ring_server.enabled:
            ringser_job = RingServerSender(
                self.settings, self.shutdown_event, self.zmq_addr
            )
            jobs.append(ringser_job)

        # Start all enabled jobs
        for job in jobs:
            job.start()

        try:
            # Monitor threads while checking for the global shutdown signal
            while not self.shutdown_event.is_set():
                for job in jobs:
                    job.join(timeout=0.1)
                    if not job.is_alive() and not self.shutdown_event.is_set():
                        logger.error(f"Manager thread {job.name} died unexpectedly")
                        self.shutdown_event.set()  # Kill everything if a core thread dies
                        break

        except Exception:
            logger.exception("Error in Managers process container")
            self.shutdown_event.set()
        finally:
            logger.info("Cleaning up Manager threads...")
            for job in jobs:
                if job.is_alive():
                    job.join(timeout=2.0)
            logger.info("Managers process stopped.")
