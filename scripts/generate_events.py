#!/usr/bin/env python3
"""Generate test events for the conflation system."""

import argparse
import json
import random
from datetime import datetime, timedelta


def generate_events(
    num_events: int,
    num_unique_ids: int,
    output_file: str = None,
    burst_mode: bool = False,
    out_of_order: bool = False,
):
    """
    Generate test events.

    Args:
        num_events: Total number of events to generate
        num_unique_ids: Number of unique event IDs
        output_file: Optional path to write events
        burst_mode: If True, all events have similar timestamps (bursty traffic)
        out_of_order: If True, randomly shuffle timestamps
    """
    events = []
    base_time = datetime.now()

    for i in range(num_events):
        # Generate timestamp
        if burst_mode:
            # All events within 1 second
            timestamp = base_time + timedelta(milliseconds=random.randint(0, 1000))
        else:
            # Spread events over time
            timestamp = base_time + timedelta(seconds=i * 0.1)

        event = {
            "id": f"object-{i % num_unique_ids}",
            "timestamp": timestamp.isoformat(),
            "payload": {
                "sequence": i,
                "random_value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
            }
        }
        events.append(event)

    # Shuffle timestamps if out-of-order mode
    if out_of_order:
        timestamps = [e["timestamp"] for e in events]
        random.shuffle(timestamps)
        for i, event in enumerate(events):
            event["timestamp"] = timestamps[i]

    # Write to file or stdout
    output = json.dumps(events, indent=2)

    if output_file:
        with open(output_file, 'w') as f:
            f.write(output)
        print(f"Generated {num_events} events to {output_file}", file=sys.stderr)
    else:
        print(output)

    return events


if __name__ == '__main__':
    import sys

    parser = argparse.ArgumentParser(description="Generate test events")
    parser.add_argument(
        '--num-events',
        '-n',
        type=int,
        default=100,
        help='Number of events to generate'
    )
    parser.add_argument(
        '--num-ids',
        '-i',
        type=int,
        default=10,
        help='Number of unique IDs'
    )
    parser.add_argument(
        '--output',
        '-o',
        help='Output file path (default: stdout)'
    )
    parser.add_argument(
        '--burst',
        action='store_true',
        help='Generate bursty traffic (all events within 1s)'
    )
    parser.add_argument(
        '--out-of-order',
        action='store_true',
        help='Shuffle timestamps to simulate out-of-order events'
    )

    args = parser.parse_args()

    generate_events(
        num_events=args.num_events,
        num_unique_ids=args.num_ids,
        output_file=args.output,
        burst_mode=args.burst,
        out_of_order=args.out_of_order,
    )
