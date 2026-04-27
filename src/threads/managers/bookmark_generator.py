import time
from datetime import UTC, datetime, timedelta
from logging import getLogger
from threading import Event, Thread

from obspy import read_events
from requests import HTTPError, post
from rpi_seism_common.settings import Settings

from src.api_models import Bookmark

logger = getLogger(__name__)


class BookmarkGenerator(Thread):
    def __init__(
        self, settings: Settings, shutdown_event: Event, earthquake_event: Event
    ):
        super().__init__()
        self.earthquake_event = earthquake_event
        self.shutdown_event = shutdown_event
        self.settings = settings
        self.bookmarks_settings = self.settings.jobs_settings.bookmark_generator

        self.last_trigger = False
        self.events: list[datetime] = []
        self.processed_ids = set()

    def run(self):
        if not self.bookmarks_settings.enabled:
            return

        logger.info("Bookmark generator started.")

        while not self.shutdown_event.is_set():
            try:
                if self.earthquake_event.is_set() and not self.last_trigger:
                    self.last_trigger = True
                    self.events.append(datetime.now(UTC))

                elif not self.earthquake_event.is_set() and self.last_trigger:
                    self.last_trigger = False

                if self.events:
                    self._request_events()

                time.sleep(10)
            except Exception:
                logger.exception("Error in Trigger Processor loop")

        logger.info("Trigger Processor stopped.")

    def _request_events(self):
        now = datetime.now(UTC)

        self.events = [i for i in self.events if now - i <= timedelta(minutes=30)]

        for i in self.events:
            start = (i - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")
            end = (i + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")

            url = self.bookmarks_settings.get_formatted_url(
                start,
                end,
                self.settings.station.latitude,
                self.settings.station.longitude,
            )

            logger.debug("Starting to search for events between %s and %s", start, end)
            self._manage_events(url)

    def _manage_events(self, url: str):
        try:
            events = read_events(url)
        except Exception:
            logger.debug("No matching events found at agency for current window.")
            return

        for event in events:
            event_id = str(event.resource_id)
            if event_id in self.processed_ids:
                continue

            origin = event.preferred_origin()
            magnitude = event.preferred_magnitude()
            event_descriptions = (
                event.event_descriptions[0] if event.event_descriptions else None
            )

            if origin is None:
                continue

            bookmark_start = origin.time - 20
            bookmark_end = origin.time + 40

            bookmark_label = f"{event_descriptions.text if event_descriptions else ''} - {magnitude.mag if magnitude else ''}{magnitude.magnitude_type if magnitude else ''}"

            agency_latitude = origin.latitude
            agency_longitude = origin.longitude
            agency_depth = origin.depth

            request_url = f"{self.bookmarks_settings.api_server_url}/bookmarks/"
            payload = Bookmark(
                label=bookmark_label,
                channels=[i.name for i in self.settings.channels],
                start=bookmark_start,
                end=bookmark_end,
                units="VEL",
            ).model_dump()

            try:
                req = post(request_url, json=payload)
                req.raise_for_status()
                logger.info("Bookmark successfully pushed: %s", bookmark_label)
                self.processed_ids.add(event_id)  # Mark as done
            except HTTPError:
                logger.error("Unable to add bookmark %s", payload)
