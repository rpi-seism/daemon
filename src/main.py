import signal
import logging
import multiprocessing  # Replaces threading
from pathlib import Path
from rpi_seism_common.settings import Settings

# Existing internal imports
from src.logger import configure_logger
from src.station_xml import ensure_station_xml
from src.jobs import Reader, MSeedWriter, WebSocketSender, TriggerProcessor, NotifierSender, RingServerSender

logger = logging.getLogger(__name__)

def main():
    data_base_folder = Path(__file__).parent.parent / "data"
    settings = Settings.load_settings(data_base_folder / "config.yml")
    configure_logger(data_base_folder)
    ensure_station_xml(settings, data_base_folder / "station.xml")

    # Use Multiprocessing Manager for shared objects
    # This allows processes to update the same Event/Tracker
    manager = multiprocessing.Manager()
    
    shutdown_event = manager.Event()
    earthquake_event = manager.Event()

    # Define the ZMQ Address
    # IPC is perfect for Raspberry Pi (low overhead, no network needed)
    ZMQ_ADDR = "ipc:///tmp/seism_hub.ipc"

    def handle_exit(sig, frame):
        logger.debug("Exit signal %s received. Shutting down...", sig)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    # Initialize Jobs as Processes
    # We no longer need to build a list of Queues!
    
    jobs = []

    # The Producer (The only one that Binds the socket)
    reader_job = Reader(
        settings,
        shutdown_event,
        ZMQ_ADDR
    )
    jobs.append(reader_job)

    # The Consumers (All connect to the same ZMQ_ADDR)
    jobs.append(MSeedWriter(settings, data_base_folder, shutdown_event, earthquake_event, ZMQ_ADDR))
    jobs.append(WebSocketSender(settings, shutdown_event, earthquake_event, ZMQ_ADDR))
    jobs.append(TriggerProcessor(settings, shutdown_event, earthquake_event, ZMQ_ADDR))
    jobs.append(NotifierSender(settings, shutdown_event, earthquake_event, ZMQ_ADDR))

    if settings.jobs_settings.ring_server.enabled:
        jobs.append(RingServerSender(settings, shutdown_event, ZMQ_ADDR))

    # Start all processes
    for job in jobs:
        job.start()

    # Wait for the Reader to finish (triggered by shutdown_event)
    try:
        reader_job.join()
    except KeyboardInterrupt:
        shutdown_event.set()

    # Wait for all other workers
    for job in jobs:
        if job.is_alive():
            job.join(timeout=2)
            job.terminate() # Force kill if they don't exit gracefully

    logger.debug("All processes stopped. Main script finished.")

if __name__ == "__main__":
    # Crucial for multiprocessing on some OS/Pi distros
    multiprocessing.set_start_method('spawn', force=True)
    main()
