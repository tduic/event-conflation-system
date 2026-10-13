"""Unit tests for configuration, models, state backends, output writers, the file adapter,
the engine's bookkeeping, and the service wiring in main.py."""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from event_conflation.config.config_manager import ConfigManager
from event_conflation.core.conflation_engine import ConflationEngine
from event_conflation.core.models import (
    ConflatedRecord,
    ConflationConfig,
    ConflationState,
    Event,
)
from event_conflation.ingestion.file_adapter import EventGenerator, FileEventAdapter
from event_conflation.ingestion.validators import EventValidator
from event_conflation.main import ConflationService, setup_logging
from event_conflation.output.output_writer import (
    ConsoleWriter,
    JSONFileWriter,
    create_output_writer,
)
from event_conflation.state.state_manager import (
    FileStateManager,
    InMemoryStateManager,
    create_state_manager,
)
from event_conflation.utils import errors

REPO = Path(__file__).resolve().parents[2]
T0 = datetime(2026, 1, 2, 3, 4, 5)


def record(id_="obj", n=3):
    return ConflatedRecord(
        id=id_,
        first_event_timestamp=T0,
        last_event_timestamp=T0 + timedelta(seconds=n),
        event_count=n,
    )


def read_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line]


# --- configuration ---------------------------------------------------------------------------


def test_shipped_config_files_load():
    test_cfg = ConfigManager.load_from_file(str(REPO / "config" / "test.yaml"))
    assert test_cfg.idle_interval == 2
    assert test_cfg.enable_batching is False
    assert test_cfg.output_file_path == "./data/test_output.json"
    assert test_cfg.log_format == "text"
    default_cfg = ConfigManager.load_from_file(str(REPO / "config" / "default.yaml"))
    assert isinstance(default_cfg, ConflationConfig)


def test_config_load_errors(tmp_path):
    with pytest.raises(FileNotFoundError):
        ConfigManager.load_from_file(str(tmp_path / "missing.yaml"))
    bad = tmp_path / "bad.yaml"
    bad.write_text("conflation: [unclosed\n")
    with pytest.raises(ValueError, match="Invalid YAML"):
        ConfigManager.load_from_file(str(bad))
    wrong_type = tmp_path / "wrong.yaml"
    wrong_type.write_text("conflation:\n  idle_interval: soon\n")
    with pytest.raises(ValueError, match="Failed to load config"):
        ConfigManager.load_from_file(str(wrong_type))
    empty = tmp_path / "empty.yaml"
    empty.write_text("")
    assert ConfigManager.load_from_file(str(empty)) == ConflationConfig()
    assert ConfigManager.load_default() == ConflationConfig()


def test_config_save_load_round_trip(tmp_path):
    cfg = ConflationConfig(
        idle_interval=1.5, max_buffer_size=7, state_backend="file", checkpoint_interval=9,
        state_file_path="s.json", checkpoint_path="cp/", output_writer="console",
        output_file_path="o.json", enable_batching=False, batch_size=3, max_clock_skew=11,
        dlq_path="d.json", log_level="DEBUG", log_format="text",
    )
    path = tmp_path / "nested" / "cfg.yaml"
    ConfigManager.save_to_file(cfg, str(path))
    assert ConfigManager.load_from_file(str(path)) == cfg


def test_from_dict_partial_sections_keep_defaults():
    cfg = ConflationConfig.from_dict({"conflation": {"idle_interval": 4}, "state": {"backend": "file"}})
    assert cfg.idle_interval == 4 and cfg.max_buffer_size == 10000
    assert cfg.state_backend == "file" and cfg.state_file_path == "./data/state.json"
    assert ConflationConfig.from_dict({}) == ConflationConfig()


def test_merge_configs_ignores_unknown_keys():
    base = ConflationConfig(idle_interval=5)
    merged = ConfigManager.merge_configs(base, {"idle_interval": 1.0, "batch_size": 7, "bogus": 1})
    assert merged.idle_interval == 1.0 and merged.batch_size == 7
    assert not hasattr(merged, "bogus")
    assert base.idle_interval == 5


# --- models ----------------------------------------------------------------------------------


def test_event_timestamp_parsing():
    assert Event(id="a", timestamp=T0).timestamp == T0
    assert Event(id="a", timestamp=T0.isoformat()).timestamp == T0
    z = Event(id="a", timestamp="2026-01-02T03:04:05Z").timestamp
    assert z.utcoffset() == timedelta(0) and z.hour == 3
    assert Event(id="a", timestamp=1_700_000_000).timestamp == datetime.fromtimestamp(1_700_000_000)
    assert Event(id="a", timestamp=1.5e9).payload == {}
    for bad in ("yesterday", None, [1, 2]):
        with pytest.raises(ValidationError):
            Event(id="a", timestamp=bad)


def test_conflated_record_requires_a_positive_count():
    assert record(n=1).event_count == 1
    with pytest.raises(ValidationError):
        record(n=0)


def test_error_hierarchy():
    for exc in (errors.ValidationError, errors.StateError, errors.BufferOverflowError,
                errors.ConfigurationError, errors.ClockSkewError):
        assert issubclass(exc, errors.ConflationError)


# --- state backends --------------------------------------------------------------------------


async def test_in_memory_state_and_checkpoint_round_trip(tmp_path):
    sm = InMemoryStateManager(checkpoint_path=str(tmp_path / "cp"))
    state = {"obj": {"id": "obj", "first_timestamp": T0, "last_update": T0, "event_count": 2,
                     "tags": [{"seen_timestamp": T0}]}}
    await sm.save_state(state)
    loaded = await sm.load_state()
    assert loaded == state and loaded is not sm.state
    assert await sm.load_latest_checkpoint() == {}
    await sm.checkpoint(state)
    restored = await sm.load_latest_checkpoint()
    assert restored == state                       # ISO strings come back as datetimes


async def test_in_memory_checkpoint_cleanup_keeps_newest(tmp_path):
    sm = InMemoryStateManager(checkpoint_path=str(tmp_path))
    for i in range(12):
        (tmp_path / f"checkpoint_20260101_0000{i:02d}.json").write_text(json.dumps({"i": i}))
    await sm._cleanup_old_checkpoints(keep=10)
    left = sorted(p.name for p in tmp_path.glob("checkpoint_*.json"))
    assert len(left) == 10 and left[0] == "checkpoint_20260101_000002.json"
    assert await sm.load_latest_checkpoint() == {"i": 11}


async def test_in_memory_corrupt_checkpoint_loads_empty(tmp_path):
    sm = InMemoryStateManager(checkpoint_path=str(tmp_path))
    (tmp_path / "checkpoint_20990101_000000.json").write_text("{nope")
    assert await sm.load_latest_checkpoint() == {}


async def test_file_state_manager(tmp_path):
    sm = FileStateManager(state_file_path=str(tmp_path / "deep" / "state.json"))
    assert await sm.load_state() == {}
    state = {"x": {"last_timestamp": T0, "note_timestamp": "not a date", "count": 1}}
    await sm.checkpoint(state)
    assert await sm.load_latest_checkpoint() == state
    sm.state_file.write_text("garbage")
    assert await sm.load_state() == {}


def test_state_manager_factory(tmp_path):
    assert isinstance(create_state_manager("memory", checkpoint_path=str(tmp_path / "c")),
                      InMemoryStateManager)
    assert isinstance(create_state_manager("file", state_file_path=str(tmp_path / "s.json")),
                      FileStateManager)
    with pytest.raises(ValueError, match="Unknown state backend"):
        create_state_manager("redis")


# --- output writers --------------------------------------------------------------------------


async def test_json_writer_batches_until_full(tmp_path):
    w = JSONFileWriter(str(tmp_path / "out" / "o.jsonl"), enable_batching=True, batch_size=3)
    for i in range(2):
        await w.write(record(f"id{i}"))
    assert not w.file_path.exists()
    assert w.get_stats() == {"total_written": 0, "buffered": 2, "file_path": str(w.file_path)}
    await w.write(record("id2"))
    assert [r["id"] for r in read_jsonl(w.file_path)] == ["id0", "id1", "id2"]
    await w.write(record("id3", n=5))
    await w.close()
    rows = read_jsonl(w.file_path)
    assert len(rows) == 4 and w.get_stats()["buffered"] == 0 and w.total_written == 4
    assert rows[-1] == {"id": "id3", "first_event_timestamp": T0.isoformat(),
                        "last_event_timestamp": (T0 + timedelta(seconds=5)).isoformat(),
                        "event_count": 5}
    await w.flush()                                 # nothing buffered: no-op
    assert len(read_jsonl(w.file_path)) == 4


async def test_json_writer_unbatched_writes_immediately(tmp_path):
    w = JSONFileWriter(str(tmp_path / "o.jsonl"), enable_batching=False)
    await w.write(record())
    assert len(read_jsonl(w.file_path)) == 1


async def test_console_writer(capsys):
    w = ConsoleWriter()
    await w.write(record("abc", n=2))
    await w.flush()
    await w.close()
    out = capsys.readouterr().out
    assert "[CONFLATED] ID: abc" in out and "Count: 2" in out
    assert w.total_written == 1


def test_output_writer_factory(tmp_path):
    assert isinstance(create_output_writer("json", file_path=str(tmp_path / "o")), JSONFileWriter)
    assert isinstance(create_output_writer("console"), ConsoleWriter)
    with pytest.raises(ValueError, match="file_path required"):
        create_output_writer("json")
    with pytest.raises(ValueError, match="Unknown writer type"):
        create_output_writer("kafka")


# --- file adapter ----------------------------------------------------------------------------


async def test_jsonl_input_with_a_bad_line_goes_to_the_dlq(tmp_path):
    f = tmp_path / "events.jsonl"
    f.write_text("\n".join([
        json.dumps({"id": "a", "timestamp": T0.isoformat()}),
        "{broken json",
        "",
        json.dumps({"id": "b"}),                                  # fails validation
        json.dumps({"id": "c", "timestamp": 1_700_000_000, "payload": {"k": 1}}),
    ]))
    v = EventValidator(dlq_path=str(tmp_path / "dlq.jsonl"))
    events = await FileEventAdapter(v).read_all_events(str(f))
    assert [e.id for e in events] == ["a", "c"]
    dlq = read_jsonl(tmp_path / "dlq.jsonl")
    assert len(dlq) == 2
    assert dlq[0]["event"] == {"raw_line": "{broken json"} and "line 2" in dlq[0]["error"]
    assert dlq[1]["event"] == {"id": "b"}
    assert v.get_stats()["rejected"] == 1 and v.get_stats()["validated"] == 2


async def test_adapter_missing_file(tmp_path):
    adapter = FileEventAdapter(EventValidator(dlq_path=str(tmp_path / "dlq")))
    with pytest.raises(FileNotFoundError):
        await adapter.read_all_events(str(tmp_path / "nope.json"))


async def test_event_generator_writes_a_readable_file(tmp_path):
    out = tmp_path / "gen" / "events.json"
    events = await EventGenerator.generate_events(25, 4, output_path=str(out))
    assert len(events) == 25 and {e["id"] for e in events} == {f"object-{i}" for i in range(4)}
    adapter = FileEventAdapter(EventValidator(dlq_path=str(tmp_path / "dlq")))
    parsed = await adapter.read_all_events(str(out))
    assert [e.payload["sequence"] for e in parsed] == list(range(25))
    assert parsed == sorted(parsed, key=lambda e: e.timestamp)


# --- engine ----------------------------------------------------------------------------------


async def test_engine_enforces_buffer_limit_and_shutdown(default_config):
    emitted = []

    async def emit(r):
        emitted.append(r)

    cfg = default_config.model_copy(update={"max_buffer_size": 2, "idle_interval": 60})
    eng = ConflationEngine(cfg, emit)
    await eng.ingest_event(Event(id="a", timestamp=T0))
    await eng.ingest_event(Event(id="b", timestamp=T0))
    await eng.ingest_event(Event(id="a", timestamp=T0 + timedelta(seconds=1)))   # existing id ok
    with pytest.raises(RuntimeError, match="Max buffer size"):
        await eng.ingest_event(Event(id="c", timestamp=T0))
    assert eng.get_stats() == {"active_processors": 2, "max_buffer_size": 2}
    states = await eng.get_all_states()
    assert states["a"]["event_count"] == 2 and states["b"]["event_count"] == 1
    await eng.shutdown()
    assert sorted((r.id, r.event_count) for r in emitted) == [("a", 2), ("b", 1)]
    assert eng.get_stats()["active_processors"] == 0
    with pytest.raises(RuntimeError, match="shutting down"):
        await eng.ingest_event(Event(id="d", timestamp=T0))


async def test_engine_restores_checkpointed_states(default_config):
    emitted = []

    async def emit(r):
        emitted.append(r)

    eng = ConflationEngine(default_config.model_copy(update={"idle_interval": 60}), emit)
    state = ConflationState(id="r", first_timestamp=T0, last_timestamp=T0 + timedelta(seconds=4),
                            event_count=4, last_update=T0)
    await eng.restore_states({"r": state.model_dump()})
    await eng.ingest_event(Event(id="r", timestamp=T0 + timedelta(seconds=9)))
    await eng.shutdown()
    assert len(emitted) == 1
    r = emitted[0]
    assert r.event_count == 5 and r.first_event_timestamp == T0
    assert r.last_event_timestamp == T0 + timedelta(seconds=9)


# --- service wiring (main.py) ----------------------------------------------------------------


async def test_service_processes_a_file_end_to_end(tmp_path):
    cfg = ConflationConfig(
        idle_interval=0.2, checkpoint_interval=3600, state_backend="memory",
        checkpoint_path=str(tmp_path / "cp"), output_file_path=str(tmp_path / "out.jsonl"),
        dlq_path=str(tmp_path / "dlq.jsonl"), enable_batching=True, batch_size=100,
    )
    events = [{"id": f"o{i % 3}", "timestamp": (T0 + timedelta(seconds=i)).isoformat()}
              for i in range(9)] + [{"id": "bad"}]
    src = tmp_path / "events.json"
    src.write_text(json.dumps(events))

    svc = ConflationService(cfg)
    await svc.initialize()
    await svc.process_file(str(src))
    await asyncio.sleep(0.6)                                    # let the idle timers fire
    await svc.shutdown()

    rows = {r["id"]: r for r in read_jsonl(cfg.output_file_path)}
    assert set(rows) == {"o0", "o1", "o2"}
    assert all(r["event_count"] == 3 for r in rows.values())
    assert rows["o0"]["first_event_timestamp"] == T0.isoformat()
    assert rows["o2"]["last_event_timestamp"] == (T0 + timedelta(seconds=8)).isoformat()
    assert len(read_jsonl(cfg.dlq_path)) == 1
    assert svc._checkpoint_task.done()
    assert list((tmp_path / "cp").glob("checkpoint_*.json"))   # final checkpoint written


async def test_service_resumes_from_checkpoint_and_flushes_on_shutdown(tmp_path):
    cfg = ConflationConfig(
        idle_interval=60, checkpoint_interval=3600, state_backend="file",
        state_file_path=str(tmp_path / "state.json"), output_file_path=str(tmp_path / "out.jsonl"),
        dlq_path=str(tmp_path / "dlq.jsonl"), enable_batching=False,
    )
    await FileStateManager(cfg.state_file_path).save_state({
        "pending": {"id": "pending", "first_timestamp": T0, "last_timestamp": T0,
                    "event_count": 7, "last_update": T0},
    })
    svc = ConflationService(cfg)
    await svc.initialize()
    assert svc.engine.get_stats()["active_processors"] == 1
    await svc.shutdown()
    assert read_jsonl(cfg.output_file_path) == [{
        "id": "pending", "first_event_timestamp": T0.isoformat(),
        "last_event_timestamp": T0.isoformat(), "event_count": 7,
    }]


def test_setup_logging_json_format():
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        root.handlers = []
        setup_logging(level="warning", log_format="json")
        assert root.level == logging.WARNING
        fmt = root.handlers[0].formatter
        rec = logging.LogRecord("x", logging.ERROR, __file__, 1, "boom %s", ("now",), None)
        data = json.loads(fmt.format(rec))
        assert data["level"] == "ERROR" and data["message"] == "boom now" and data["logger"] == "x"
        try:
            raise KeyError("k")
        except KeyError:
            import sys
            rec.exc_info = sys.exc_info()
        assert "KeyError" in json.loads(fmt.format(rec))["exception"]
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)
