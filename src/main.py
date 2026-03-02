import signal

from queue import Queue
from pathlib import Path
from threading import Event
import logging

from src.settings import Settings
from src.jobs import Reader, MSeedWriter, WebSocketSender, TriggerProcessor, NotifierSender


logger = logging.getLogger(__name__)


def main():
    """
    Main function that initializes the seismic data acquisition system.
    It sets up logging, loads settings, creates necessary threads for reading data,
    writing to MiniSEED files, sending data over WebSocket, and processing triggers.
    It also handles graceful shutdown on receiving termination signals.
    """
    # Define paths and load settings
    data_base_folder = Path(__file__).parent.parent / "data"
    settings = Settings.load_settings()

    # Create a global shutdown event
    shutdown_event = Event()
    earthquake_event = Event()

    # Define a signal handler for systemd (SIGTERM)
    def handle_exit(sig, frame):
        logger.debug("Exit signal %s received. Shutting down...", sig)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    # Create queues for communication between jobs
    msed_writer_queue = Queue()
    websocket_queue = Queue()
    trigger_queue = Queue()
    notifier_queue = Queue()

    # Create and start the Reader job thread (reads from ADC, puts data in the queues)
    reader_job = Reader(
        "/dev/ttyUSB0",
        settings,
        [msed_writer_queue, websocket_queue, trigger_queue, notifier_queue],
        shutdown_event
    )
    reader_job.start()

    # Create and start the MSeedWriter job thread (writes data to MiniSEED file)
    m_seed_writer_job = MSeedWriter(
        settings,
        msed_writer_queue,
        data_base_folder,
        shutdown_event,
        earthquake_event,
        write_interval_sec=1800
    )
    m_seed_writer_job.start()

    # Create and start the WebSocketSender job thread (sends data over WebSocket)
    websocket_job = WebSocketSender(
        settings,
        websocket_queue,
        shutdown_event,
        earthquake_event,
        host="0.0.0.0"
    )
    websocket_job.start()

    # Create and start the TriggerProcessor job thread (sends data over WebSocket)
    trigger_processor_job = TriggerProcessor(
        settings,
        trigger_queue,
        shutdown_event,
        earthquake_event
    )
    trigger_processor_job.start()

        # Create and start the TriggerProcessor job thread (sends data over WebSocket)
    notifier_job = NotifierSender(
        settings,
        notifier_queue,
        shutdown_event,
        earthquake_event
    )
    notifier_job.start()


    # Gracefully stop all threads
    reader_job.join()

    # Wait for all threads to finish
    m_seed_writer_job.join()
    websocket_job.join()
    trigger_processor_job.join()
    notifier_job.join()

    logger.debug("All threads stopped and the main script has finished.")


if __name__ == "__main__":
    main()
