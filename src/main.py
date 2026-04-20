import logging
import multiprocessing
import signal
import time
from pathlib import Path

from rpi_seism_common.settings import Settings

# Internal imports for the new Process Containers
from src.logger import configure_logger
from src.processes.managers import Managers
from src.processes.plotters import Plotters
from src.processes.producers import Producers
from src.station_xml import ensure_station_xml

logger = logging.getLogger(__name__)


def main():
    # 1. Setup paths and settings
    data_base_folder = Path(__file__).parent.parent / "data"
    settings = Settings.load_settings(data_base_folder / "config.yml")
    configure_logger(data_base_folder)
    ensure_station_xml(settings, data_base_folder / "station.xml")

    # Use standard primitives (Faster, direct IPC)
    shutdown_event = multiprocessing.Event()
    earthquake_event = multiprocessing.Event()
    plot_queue = multiprocessing.Queue()

    # Define the ZMQ Address for IPC
    ZMQ_ADDR = "ipc:///tmp/seism_hub.ipc"

    # 3. Signal Handling
    def handle_exit(sig, frame):
        if not shutdown_event.is_set():
            logger.info(f"Signal {sig} received. Initiating graceful shutdown...")
            shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    # 4. Initialize the 3 Process Containers
    # Each of these encapsulates multiple threads/tasks

    producers = Producers(
        settings,
        data_base_folder,
        shutdown_event,
        earthquake_event,
        plot_queue,
        ZMQ_ADDR,
    )

    managers = Managers(settings, shutdown_event, earthquake_event, ZMQ_ADDR)

    plotters = Plotters(plot_queue, shutdown_event)

    all_processes = [producers, managers, plotters]

    # 5. Start Execution
    logger.info("Launching Seismic Stack (3-Process Architecture)...")
    for p in all_processes:
        p.start()

    # 6. Monitor and Wait
    try:
        # Keep main alive while the core producer is running
        while not shutdown_event.is_set():
            if not producers.is_alive():
                logger.error("Producers process died! Shutting down system.")
                shutdown_event.set()
                break
            time.sleep(1)

    except KeyboardInterrupt:
        shutdown_event.set()
    finally:
        logger.info("Cleaning up processes...")

        # Give processes a moment to finish their loops
        for p in all_processes:
            p.join(timeout=30)
            if p.is_alive():
                logger.warning(f"Process {p.name} refused to exit. Terminating...")
                p.terminate()

    logger.info("Main script finished.")


if __name__ == "__main__":
    # 'spawn' is safest for hardware and ZMQ inside Docker
    multiprocessing.set_start_method("spawn", force=True)
    main()
