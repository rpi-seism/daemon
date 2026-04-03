from threading import Lock
from time import time


class SOHTracker:
    """
    Thread-safe state-of-health metrics tracker for the serial link.
    Tracks packet success/failure, checksum errors, and connection status.
    """
    def __init__(self):
        self._lock = Lock()
        self._total_packets = 0
        self._successful_packets = 0
        self._checksum_errors = 0
        self._bytes_dropped = 0
        self._last_seen = 0.0
        self._connected = False

    def record_success(self):
        """Record a successfully received and validated packet."""
        with self._lock:
            self._total_packets += 1
            self._successful_packets += 1
            self._last_seen = time()
            self._connected = True

    def record_checksum_error(self):
        """Record a packet that failed checksum validation."""
        with self._lock:
            self._total_packets += 1
            self._checksum_errors += 1

    def record_dropped_bytes(self, count: int = 1):
        """Record bytes that were discarded while searching for packet headers."""
        with self._lock:
            self._bytes_dropped += count

    def set_disconnected(self):
        """Mark the link as disconnected."""
        with self._lock:
            self._connected = False

    def get_snapshot(self) -> dict:
        """
        Return a snapshot of current SOH metrics.
        
        Returns:
            dict with keys: link_quality, bytes_dropped, checksum_errors, 
                           last_seen, connected
        """
        with self._lock:
            # Calculate link quality as ratio of successful packets
            if self._total_packets > 0:
                link_quality = self._successful_packets / self._total_packets
            else:
                link_quality = 0.0

            return {
                "link_quality": round(link_quality, 3),
                "bytes_dropped": self._bytes_dropped,
                "checksum_errors": self._checksum_errors,
                "last_seen": self._last_seen,
                "connected": self._connected,
            }
