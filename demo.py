#!/usr/bin/env python3
"""
Quick demo of the event conflation system.

This script:
1. Generates sample events
2. Runs the conflation engine
3. Displays the results
"""

import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path


async def main():
    print("=" * 70)
    print("Event Conflation System - Demo")
    print("=" * 70)
    print()

    # Add src to path
    sys.path.insert(0, str(Path(__file__).parent / "src"))

    from event_conflation.core.models import Event, ConflationConfig
    from event_conflation.core.conflation_engine import ConflationEngine
    from event_conflation.output.output_writer import ConsoleWriter

    # Create config with short idle interval for demo
    config = ConflationConfig(
        idle_interval=2.0,  # 2 seconds for quick demo
        max_buffer_size=100,
        log_level="INFO",
    )

    print(f"Configuration:")
    print(f"  - Idle Interval: {config.idle_interval}s")
    print(f"  - Max Buffer Size: {config.max_buffer_size}")
    print()

    # Create output writer (console for demo)
    writer = ConsoleWriter()

    async def emit_callback(record):
        await writer.write(record)

    # Create engine
    engine = ConflationEngine(config=config, emit_callback=emit_callback)

    print("Generating sample events...")
    print()

    # Generate sample events
    base_time = datetime.now()
    events = []

    # Object-1: 3 events in burst
    for i in range(3):
        events.append(
            Event(
                id="object-1",
                timestamp=base_time + timedelta(seconds=i * 0.5),
                payload={"sequence": i, "type": "burst"}
            )
        )

    # Object-2: 2 events
    for i in range(2):
        events.append(
            Event(
                id="object-2",
                timestamp=base_time + timedelta(seconds=i * 0.3),
                payload={"sequence": i, "type": "pair"}
            )
        )

    # Object-3: single event
    events.append(
        Event(
            id="object-3",
            timestamp=base_time,
            payload={"type": "single"}
        )
    )

    print(f"Generated {len(events)} events for 3 unique IDs")
    print()

    # Ingest events
    print("Ingesting events into conflation engine...")
    for event in events:
        await engine.ingest_event(event)
        print(f"  → Ingested: {event.id} at {event.timestamp.strftime('%H:%M:%S.%f')[:-3]}")

    print()
    print(f"Waiting {config.idle_interval}s for timers to expire...")
    print()

    # Wait for idle interval
    await asyncio.sleep(config.idle_interval + 0.5)

    print()
    print("=" * 70)
    print("Demo complete!")
    print()
    print("Summary:")
    print(f"  - Input: {len(events)} events across 3 IDs")
    print(f"  - Output: 3 conflated records")
    print(f"  - object-1: 3 events conflated")
    print(f"  - object-2: 2 events conflated")
    print(f"  - object-3: 1 event conflated")
    print("=" * 70)

    # Cleanup
    await engine.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
        sys.exit(0)
