# State of Health (SOH) Implementation Guide

This implementation adds real-time State of Health monitoring to the rpi-seism system, tracking serial link quality and displaying metrics in a PrimeNG popover.

## Architecture Overview

The SOH system consists of three main components:

1. **SOHTracker** (`src/jobs/soh_tracker.py`)
   - Thread-safe metrics collector
   - Tracks: link quality, checksum errors, bytes dropped, last seen timestamp, connection status
   - Provides atomic snapshots of current metrics

2. **Reader** (updated in `src/jobs/reader.py`)
   - Records SOH metrics during packet processing:
     - `record_success()` — valid packet received
     - `record_checksum_error()` — CRC validation failed
     - `record_dropped_bytes(count)` — bytes discarded while searching for headers
     - `set_disconnected()` — connection lost

3. **WebSocketSender** (updated in `src/jobs/websocket_sender.py`)
   - Broadcasts SOH message every 5 seconds
   - Uses existing `StateOfHealth` WebSocket message structure
   - Sends to all connected clients

## Metrics Explained

### Link Quality
Ratio of successfully validated packets to total packets received. 
- Formula: `successful_packets / total_packets`
- Range: 0.0 to 1.0 (displayed as percentage)
- Thresholds:
  - **Excellent** (green): >95%
  - **Fair** (amber): 80-95%
  - **Poor** (red): <80%

### Checksum Errors
Count of packets that failed CRC32 validation. Indicates:
- Electrical interference on the serial line
- Grounding issues
- Cable quality problems
- EMI from nearby HF/VHF equipment

### Bytes Dropped
Number of bytes discarded while searching for packet headers (0xAA 0xBB).
- Higher values indicate:
  - Out-of-sync condition
  - Partial packet loss
  - UART buffer overruns

### Last Seen
Unix timestamp (in seconds) of the most recent successfully validated packet.
- Used to detect stale connections
- "Seconds ago" display helps identify recent issues

### Connected
Boolean flag indicating whether the Reader thread is actively receiving data.
- Set to `false` when Reader encounters exceptions or shuts down

## Implementation Details

### Backend Flow

1. **Reader Thread** processes incoming serial data:
   ```python
   # On successful packet
   self.soh_tracker.record_success()
   
   # On checksum failure
   self.soh_tracker.record_checksum_error()
   
   # On dropped byte
   self.soh_tracker.record_dropped_bytes(1)
   
   # On disconnect
   self.soh_tracker.set_disconnected()
   ```

2. **WebSocketSender** broadcasts SOH every 5 seconds:
   ```python
   async def _broadcast_soh(self):
       snapshot = self.soh_tracker.get_snapshot()
       payload = StateOfHealthPayload(**snapshot)
       message = StateOfHealth(payload=payload)
       await self._broadcast(message)
   ```

3. **Main Thread** creates SOHTracker and passes it to both Reader and WebSocketSender:
   ```python
   soh_tracker = SOHTracker()
   
   reader_job = Reader(settings, queues, shutdown_event, soh_tracker)
   websocket_job = WebSocketSender(settings, websocket_queue, 
                                   shutdown_event, earthquake_event, 
                                   soh_tracker, host="0.0.0.0")
   ```

## Testing Recommendations

1. **Normal Operation**: Verify SOH popover shows >95% link quality with green indicators

2. **Checksum Errors**: Introduce electrical noise near the Cat5/6 cable to trigger checksum failures

3. **Bytes Dropped**: Temporarily disconnect/reconnect serial to force out-of-sync condition

4. **Stale Connection**: Kill the daemon and watch "seconds ago" increment while connection status turns red

5. **Reconnection**: Restart daemon and verify metrics reset and connection status turns green

## Performance Impact

- **Backend**: Negligible — atomic counter increments, single snapshot every 5s
- **Frontend**: Minimal — single WebSocket message per 5s, 1Hz UI update for "seconds ago"
- **Network**: ~100 bytes per SOH message

## Future Enhancements

Potential additions (not implemented):

1. **Historical graphs**: Chart link quality over time
2. **SOH logging**: Write metrics to JSON file every 60s
3. **Alert thresholds**: Send Telegram notification when link quality drops below threshold
4. **Reset counters**: Button to zero cumulative error counts
5. **Extended metrics**: 
   - UART buffer utilization
   - Packet rate (packets/second)
   - Average latency (timestamp delta between MCU and daemon)
