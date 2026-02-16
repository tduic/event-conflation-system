# Quick Start Guide

## Installation

```bash
# Clone the repository
git clone https://github.com/tduic/event-conflation-system.git
cd event-conflation-system

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

**Note:** Always make sure the virtual environment is activated before running commands:
```bash
source venv/bin/activate  # Run this first if you see "ModuleNotFoundError"
```

## Run the Demo

```bash
# Quick 2-second demo
python demo.py
```

**Expected Output:**
```
======================================================================
Event Conflation System - Demo
======================================================================

Configuration:
  - Idle Interval: 2.0s
  - Max Buffer Size: 100

Generating sample events...
Generated 6 events for 3 unique IDs

Ingesting events into conflation engine...
  → Ingested: object-1 at 18:37:20.122
  → Ingested: object-1 at 18:37:20.622
  → Ingested: object-1 at 18:37:21.122
  ...

[CONFLATED] ID: object-1
  First: 2026-02-16T18:37:20.122422
  Last:  2026-02-16T18:37:21.122422
  Count: 3
```

## Run with Real Data

```bash
# 1. Generate test events
python scripts/generate_events.py -n 50 -i 5 -o events.json

# 2. Run conflation (with 2-second idle interval for testing)
python -m event_conflation.main --config config/test.yaml --events events.json

# 3. View output
cat data/test_output.json
```

**Sample Output:**
```json
{"id": "object-0", "first_event_timestamp": "2026-02-16T18:39:47.478178", "last_event_timestamp": "2026-02-16T18:39:51.978178", "event_count": 10}
{"id": "object-1", "first_event_timestamp": "2026-02-16T18:39:47.578178", "last_event_timestamp": "2026-02-16T18:39:52.078178", "event_count": 10}
...
```

## Run Tests

```bash
# All tests
pytest tests/ -v --cov=src/event_conflation

# Unit tests only
pytest tests/unit/ -v

# Integration tests only
pytest tests/integration/ -v
```

**Expected Result:**
```
======================== 34 passed in 7.67s ========================
Coverage: 54%
```

## Generate Events for Testing

```bash
# Standard events
python scripts/generate_events.py -n 100 -i 10 -o events.json

# Bursty traffic (all events within 1 second)
python scripts/generate_events.py -n 1000 -i 100 --burst -o burst_events.json

# Out-of-order timestamps
python scripts/generate_events.py -n 100 -i 10 --out-of-order -o ooo_events.json
```

## Configuration

Edit `config/default.yaml` or `config/test.yaml`:

```yaml
conflation:
  idle_interval: 30  # Seconds to wait before emitting

output:
  file_path: "./data/conflated_events.json"

logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR
```

## Project Structure

```
quantinno-event-conflation/
├── src/event_conflation/    # Source code
├── tests/                   # Test suite (34 tests)
├── config/                  # YAML configurations
├── scripts/                 # Event generator
├── demo.py                  # Quick demo
└── README.md               # Full documentation
```

## Next Steps

1. Read [README.md](README.md) for architecture and design details
2. Read [docs/AI_TOOLING.md](docs/AI_TOOLING.md) for development process
3. Explore the code in `src/event_conflation/`
4. Run the test suite to understand behavior
5. Try different configurations and event patterns

## Key Features Demonstrated

✅ **AsyncIO-based timer management** - Efficient concurrent processing
✅ **Robust error handling** - DLQ, clock skew detection, graceful shutdown
✅ **Production-ready** - Comprehensive tests, checkpointing, logging
✅ **Scalable design** - Multi-node architecture documented
✅ **Well-tested** - 34 tests, 54% coverage, all core components >75%

---

For questions or issues, see the main [README.md](README.md)
