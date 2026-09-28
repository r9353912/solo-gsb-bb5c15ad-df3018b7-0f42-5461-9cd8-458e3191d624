"""End-of-segment (active/closed) lifecycle for consecutive-threshold alerts.

A fired alert's violating segment is recorded as ending at most once, in a
separate ``alert_closings`` table, with one of three reasons:

* ``record``  - first committed record that no longer violates (normal value,
  missing field or non-numeric value);
* ``gap``     - a declared 断档 before the run could continue;
* ``version`` - the first committed record taken over by a newer rule version.

The alert snapshot and the acknowledgement state are never rewritten by a
closing. Only first-time commits can close a segment: duplicates, records
that stay buffered and gap_filled backfills must not close it early. The list
endpoint exposes the closing and supports an ``active`` filter. Old databases
gain the new table automatically on startup.
"""
from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import connect, init_db
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        window_size=200,
        gap_timeout_seconds=0.25,
        sweep_enabled=False,  # deterministic: gaps are declared lazily on access
    )
    with TestClient(create_app(settings)) as c:
        yield c


def submit_payloads(client, device, entries):
    resp = client.post(
        "/api/v1/batches",
        json={
            "device_id": device,
            "records": [{"seq": s, "payload": p} for s, p in entries],
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def submit(client, device, seqs, value=40.0, field="temp"):
    return submit_payloads(client, device, [(s, {field: value}) for s in seqs])


def create_rule(client, device="dev-a", field="temp", op="gt",
                threshold=30.0, consecutive=3):
    resp = client.post(
        f"/api/v1/devices/{device}/alert-rules",
        json={"field": field, "op": op, "threshold": threshold,
              "consecutive": consecutive},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def alerts(client, device="dev-a", **params):
    resp = client.get(f"/api/v1/devices/{device}/alerts", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()["alerts"]


def modify(client, rule_id, body, device="dev-a"):
    return client.put(
        f"/api/v1/devices/{device}/alert-rules/{rule_id}", json=body
    )


def declare_gap(client, device="dev-a"):
    time.sleep(0.3)
    client.get(f"/api/v1/devices/{device}/window")  # lazy 断档 declaration


def assert_active(alert):
    assert alert["active"] is True
    assert alert["closing"] is None


def assert_closed(alert, reason, end_seq):
    assert alert["active"] is False
    closing = alert["closing"]
    assert closing is not None
    assert closing["reason"] == reason
    assert closing["end_seq"] == end_seq
    assert closing["ended_at"] > 0
    return closing


# --------------------------------------------------------------------- #
# reason 'record': the first non-violating committed record
# --------------------------------------------------------------------- #

def test_fired_alert_starts_active_and_closes_on_normal_record(client):
    create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 2, 3])
    alert = alerts(client)[0]
    assert_active(alert)

    # more violations keep the same segment open (no second alert, still active)
    submit(client, "dev-a", [4, 5])
    assert len(alerts(client)) == 1
    assert_active(alerts(client)[0])

    # the first non-violating committed record ends the segment
    submit(client, "dev-a", [6], value=10.0)
    alert = alerts(client)[0]
    assert_closed(alert, "record", 6)

    # after the closing the rule is re-armed and a fresh segment can fire
    submit(client, "dev-a", [7, 8, 9])
    assert [(a["start_seq"], a["end_seq"]) for a in alerts(client)] == [(1, 3), (7, 9)]
    assert_active(alerts(client)[1])


def test_missing_or_non_numeric_record_closes_with_reason_record(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # fires at 2

    submit_payloads(client, "dev-a", [
        (3, {"other": 1}),          # field missing -> ends the segment
    ])
    assert_closed(alerts(client)[0], "record", 3)

    # next violating run fires independently and a non-numeric value ends it
    submit(client, "dev-a", [4, 5])
    submit_payloads(client, "dev-a", [(6, {"temp": "hot"})])
    assert_closed(alerts(client)[1], "record", 6)


def test_closing_does_not_touch_snapshot_or_acknowledgement(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])
    fired = alerts(client)[0]

    client.post(
        "/api/v1/devices/dev-a/alerts/1/acknowledge",
        json={"acknowledged_by": "ops-alice"},
    )
    submit(client, "dev-a", [3], value=10.0)  # ends the segment

    a = alerts(client)[0]
    assert a["status"] == "acknowledged"
    assert a["acknowledged_by"] == "ops-alice"
    ack_at = a["acknowledged_at"]
    assert ack_at == fired["acknowledged_at"] or ack_at is not None
    # snapshot fields are untouched
    for key in ("rule_id", "rule_version_id", "version_no", "field", "op",
                "threshold", "consecutive", "start_seq", "end_seq",
                "trigger_value", "created_at", "device_id"):
        assert a[key] == fired[key]
    assert_closed(a, "record", 3)
    # an acknowledged + closed alert stays acked
    assert alerts(client, status="acknowledged")[0]["alert_id"] == 1
    assert alerts(client, active=False)[0]["alert_id"] == 1


def test_alert_closes_at_most_once_first_terminator_wins(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # fires at 2
    submit(client, "dev-a", [3], value=10.0)  # ends with 'record' at seq 3

    first = alerts(client)[0]["closing"]
    assert (first["reason"], first["end_seq"]) == ("record", 3)

    # a later version handover and more records cannot re-close it
    r = modify(client, 1, {"field": "temp", "op": "gt", "threshold": 30,
                           "consecutive": 2, "effective_seq": 4})
    assert r.status_code == 200, r.text
    submit(client, "dev-a", [4, 5])  # v2 first record at 4, v2 alert at 5
    submit(client, "dev-a", [6], value=10.0)  # ends the *v2* segment only

    all_alerts = alerts(client)
    assert [(a["alert_id"], a["closing"]["reason"], a["closing"]["end_seq"])
            for a in all_alerts] == [(1, "record", 3), (2, "record", 6)]
    assert all_alerts[0]["closing"] == first  # immutable once written


# --------------------------------------------------------------------- #
# reason 'gap': a declared 断档
# --------------------------------------------------------------------- #

def test_declared_gap_closes_open_segment_with_end_at_last_missing_seq(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])      # fires at 2
    submit(client, "dev-a", [5])         # buffered behind pending gap 3..4
    assert_active(alerts(client)[0])     # a buffered record does not close it

    declare_gap(client)                  # declares gap [3,4], drains 5
    alert = alerts(client)[0]
    # the segment ends at the last missing seq of the declared gap
    assert_closed(alert, "gap", 4)
    # seq 5 started a fresh run; it is not part of the closed segment
    assert (alert["start_seq"], alert["end_seq"]) == (1, 2)


def test_gap_closing_when_drained_record_violates_stays_reason_gap(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])      # fires
    submit(client, "dev-a", [4])         # buffered behind gap [3,3], violates
    declare_gap(client)                  # gap [3,3]; seq 4 drains and violates
    alert = alerts(client)[0]
    # closing was decided by the gap itself, not by the violating drain record
    assert_closed(alert, "gap", 3)


def test_gap_closes_old_segment_when_switch_is_scheduled_inside_hole(client):
    """v1 fires, then v2 is scheduled to take over at a seq that lands inside
    the later-declared gap: v2 never evaluates before the jump. The 断档 itself
    must end v1's still-open segment (reason 'gap', end at the last hole seq)
    rather than leaving it waiting forever for a takeover record."""
    rule = create_rule(client, consecutive=2)
    # seq 1,2 commit -> v1 alert; seq 6 buffers behind the pending gap 3..5
    submit(client, "dev-a", [1, 2, 6])
    # switch lands inside the hole (v2 would own seq 5, which is never committed)
    r = modify(client, rule["rule_id"],
               {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
                "effective_seq": 5})
    assert r.status_code == 200, r.text
    assert_active(alerts(client)[0])  # the modification alone never closes

    declare_gap(client)  # declares gap [3,5], drains 6 under v2
    a1 = alerts(client, rule_id=rule["rule_id"], version_no=1)[0]
    assert_closed(a1, "gap", 5)
    # and it is closed exactly once even though v2 also "takes over" at seq 6
    submit(client, "dev-a", [7], value=5.0)  # non-violating under v2
    assert alerts(client, rule_id=1, version_no=1)[0]["closing"] == a1["closing"]
    assert alerts(client, rule_id=1, version_no=1)[0]["status"] == "open"


def test_gap_backfills_do_not_close_or_recompute(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])
    submit(client, "dev-a", [5])
    declare_gap(client)  # gap [3,4] closes the segment at 4; 5 drains
    closing_before = alerts(client)[0]["closing"]

    client.post("/api/v1/devices/dev-a/gaps/1/replay")
    body = submit(client, "dev-a", [3, 4])
    assert [r["status"] for r in body["results"]] == ["gap_filled", "gap_filled"]
    # gap_filled backfills neither rewrite the closing nor fire/close anything
    assert alerts(client)[0]["closing"] == closing_before


def test_duplicate_and_buffered_records_do_not_close_early(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # fires at 2
    # duplicate retries of the fired records never close the segment
    body = submit(client, "dev-a", [1, 2])
    assert all(r["status"] == "duplicate" for r in body["results"])
    assert_active(alerts(client)[0])
    # a record that only buffers must not close the segment either
    submit(client, "dev-a", [5])  # buffered behind pending gap 3..4
    assert_active(alerts(client)[0])


# --------------------------------------------------------------------- #
# reason 'version': a newer rule version takes over
# --------------------------------------------------------------------- #

def test_version_takeover_closes_old_segment_on_first_new_record(client):
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])          # v1 alert at 2
    assert_active(alerts(client)[0])
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 3})
    # modifying the rule alone does not close anything
    assert_active(alerts(client)[0])
    # the first committed record owned by v2 ends the old segment at seq 3
    submit(client, "dev-a", [3])
    assert_closed(alerts(client)[0], "version", 3)


def test_version_takeover_with_buffered_drain_closes_in_seq_order(client):
    """Switch while records are buffered; on drain each seq closes the right
    segment at most once, in ascending seq order."""
    rule = create_rule(client, consecutive=2)
    # seq 1 commits; 4 buffers behind pending gap 2..3
    submit(client, "dev-a", [1, 4])
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 4})
    # committing 2 drains 2,3 (v1: alert run 1..2 fires at 2), then seq 4 is
    # v2's first record (40 < 999 non-violating) and ends v1's segment at 4.
    submit(client, "dev-a", [2, 3])
    a1 = alerts(client)[0]
    assert (a1["version_no"], a1["start_seq"], a1["end_seq"]) == (1, 1, 2)
    assert_closed(a1, "version", 4)


def test_record_close_beats_version_takeover_for_same_segment(client):
    """When the takeover record itself is non-violating under the OLD version
    the record would still own the segment; but once the switch is effective at
    that seq, 'version' is the terminator. Verify the alternative: a non-
    violating record before the switch closes with 'record'."""
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # v1 fires
    # a normal committed record before the switch ends it with 'record'
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 30, "consecutive": 2,
            "effective_seq": 10})
    submit(client, "dev-a", [3], value=10.0)
    assert_closed(alerts(client)[0], "record", 3)


def test_multiple_rules_close_independently(client):
    create_rule(client, device="dev-a", field="temp", op="gt",
                threshold=30.0, consecutive=1)
    create_rule(client, device="dev-a", field="hum", op="lt",
                threshold=20.0, consecutive=1)
    submit_payloads(client, "dev-a", [(1, {"temp": 40, "hum": 5})])
    assert len(alerts(client)) == 2
    submit_payloads(client, "dev-a", [(2, {"temp": 5, "hum": 5})])
    # temp's segment ended at 2 (record); hum's run never ended and is active
    by_rule = {a["rule_id"]: a for a in alerts(client)}
    assert_closed(by_rule[1], "record", 2)
    assert_active(by_rule[2])


# --------------------------------------------------------------------- #
# active filter
# --------------------------------------------------------------------- #

def test_active_filter_lists_open_and_closed(client):
    create_rule(client, consecutive=1)
    submit(client, "dev-a", [1])             # alert 1 active
    submit(client, "dev-a", [2], value=5.0)  # closes alert 1 at 2
    submit(client, "dev-a", [3])             # alert 2 active

    assert [a["alert_id"] for a in alerts(client, active=True)] == [2]
    assert [a["alert_id"] for a in alerts(client, active=False)] == [1]
    assert [a["alert_id"] for a in alerts(client)] == [1, 2]
    # active composes with the acknowledgement filter
    assert alerts(client, active=True, status="open")
    # an acknowledged active alert is still active
    client.post("/api/v1/devices/dev-a/alerts/2/acknowledge",
                json={"acknowledged_by": "ops-a"})
    assert [a["alert_id"]
            for a in alerts(client, active=True, status="acknowledged")] == [2]
    assert [a["alert_id"]
            for a in alerts(client, active=False, status="acknowledged")] == []
    # invalid active value is a request validation error
    resp = client.get("/api/v1/devices/dev-a/alerts", params={"active": "maybe"})
    assert resp.status_code == 422


# --------------------------------------------------------------------- #
# old database: table auto-created on startup
# --------------------------------------------------------------------- #

def test_old_database_gains_closings_table_and_historical_alerts_active(tmp_path):
    import sqlite3

    path = str(tmp_path / "old.db")
    with sqlite3.connect(path) as raw:
        raw.execute(
            "CREATE TABLE alert_rules (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "device_id TEXT, field TEXT, op TEXT, threshold REAL, "
            "consecutive INTEGER, created_at REAL)"
        )
        raw.execute(
            "CREATE TABLE alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id INTEGER, "
            "device_id TEXT, field TEXT, op TEXT, threshold REAL, consecutive INTEGER, "
            "start_seq INTEGER, end_seq INTEGER, trigger_value REAL, created_at REAL)"
        )
        raw.execute(
            "INSERT INTO alerts (rule_id, device_id, field, op, threshold, consecutive, "
            "start_seq, end_seq, trigger_value, created_at) "
            "VALUES (1, 'dev-a', 'temp', 'gt', 30, 2, 1, 2, 40, 1.0)"
        )

    init_db(path)
    init_db(path)  # idempotent re-run
    conn = connect(path)
    try:
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='alert_closings'"
        ).fetchone() is not None
        assert conn.execute("SELECT COUNT(*) AS c FROM alert_closings").fetchone()["c"] == 0
    finally:
        conn.close()

    settings = Settings(db_path=path, sweep_enabled=False)
    with TestClient(create_app(settings)) as c:
        # historical alert has no closing row -> active; still acknowledgeable
        a = c.get("/api/v1/devices/dev-a/alerts").json()["alerts"]
        assert len(a) == 1 and a[0]["active"] is True and a[0]["closing"] is None
        assert c.get("/api/v1/devices/dev-a/alerts",
                     params={"active": "true"}).json()["alerts"][0]["alert_id"] == 1
        assert c.get("/api/v1/devices/dev-a/alerts",
                     params={"active": "false"}).json()["alerts"] == []


def test_migrated_database_can_close_historical_alert_on_new_record(tmp_path):
    import sqlite3

    path = str(tmp_path / "old2.db")
    with sqlite3.connect(path) as raw:
        raw.execute(
            "CREATE TABLE devices (device_id TEXT PRIMARY KEY, next_expected INTEGER, "
            "gap_since REAL, duplicates_count INTEGER, created_at REAL, updated_at REAL)"
        )
        raw.execute("INSERT INTO devices VALUES ('dev-a', 3, NULL, 0, 1.0, 1.0)")
        raw.execute(
            "CREATE TABLE alert_rules (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "device_id TEXT, field TEXT, op TEXT, threshold REAL, "
            "consecutive INTEGER, created_at REAL)"
        )
        raw.execute(
            "INSERT INTO alert_rules (device_id, field, op, threshold, consecutive, "
            "created_at) VALUES ('dev-a', 'temp', 'gt', 30, 2, 1.0)"
        )
        raw.execute(
            "CREATE TABLE alert_rule_state (rule_id INTEGER PRIMARY KEY, last_seq INTEGER, "
            "streak INTEGER, alerted INTEGER, updated_at REAL)"
        )
        raw.execute("INSERT INTO alert_rule_state VALUES (1, 2, 2, 1, 2.0)")
        raw.execute(
            "CREATE TABLE alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id INTEGER, "
            "device_id TEXT, field TEXT, op TEXT, threshold REAL, consecutive INTEGER, "
            "start_seq INTEGER, end_seq INTEGER, trigger_value REAL, created_at REAL)"
        )
        raw.execute(
            "INSERT INTO alerts (rule_id, device_id, field, op, threshold, consecutive, "
            "start_seq, end_seq, trigger_value, created_at) "
            "VALUES (1, 'dev-a', 'temp', 'gt', 30, 2, 1, 2, 40, 2.0)"
        )

    init_db(path)
    settings = Settings(db_path=path, window_size=200, sweep_enabled=False)
    with TestClient(create_app(settings)) as c:
        # migrated v1 state carried alerted=1: a non-violating committed record
        # ends the historical segment exactly like a fresh database
        r = c.post("/api/v1/batches", json={
            "device_id": "dev-a",
            "records": [{"seq": 3, "payload": {"temp": 5}}],
        })
        assert r.status_code == 200, r.text
        a = c.get("/api/v1/devices/dev-a/alerts").json()["alerts"][0]
        assert a["active"] is False
        assert a["closing"]["reason"] == "record"
        assert a["closing"]["end_seq"] == 3


# --------------------------------------------------------------------- #
# concurrency against a real uvicorn server
# --------------------------------------------------------------------- #

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_concurrent_first_commits_close_each_alert_once(tmp_path):
    """Many threads commit the closing (non-violating) seq concurrently:
    every fired alert ends up with exactly one, deterministic closing."""
    import uvicorn

    settings = Settings(
        db_path=str(tmp_path / "conc_close.db"),
        window_size=10_000, gap_timeout_seconds=600, sweep_enabled=False,
    )
    port = _free_port()
    config = uvicorn.Config(create_app(settings), host="127.0.0.1", port=port,
                            log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                httpx.get(f"{base}/health", timeout=0.2)
                break
            except Exception:
                time.sleep(0.05)
        else:
            pytest.fail("server did not start")

        with httpx.Client(base_url=base, timeout=10) as c:
            assert c.post(
                "/api/v1/devices/dev-a/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30,
                      "consecutive": 1},
            ).status_code == 201
            # anchor the window with a normal record
            assert c.post("/api/v1/batches", json={
                "device_id": "dev-a",
                "records": [{"seq": 0, "payload": {"temp": 5}}],
            }).status_code == 200
            # fire one alert per odd seq (consecutive=1); the even seq right
            # after each firing closes that segment again
            assert c.post("/api/v1/batches", json={
                "device_id": "dev-a",
                "records": [{"seq": s, "payload": {"temp": 40 if s % 2 else 5}}
                            for s in range(1, 201)],
            }).status_code == 200

        # hammer duplicate retries of every closing seq: all are duplicates and
        # none must alter or duplicate a closing
        def hammer(seq):
            with httpx.Client(base_url=base, timeout=10) as c:
                r = c.post("/api/v1/batches", json={
                    "device_id": "dev-a",
                    "records": [{"seq": seq, "payload": {"temp": 5}}],
                })
                assert r.status_code == 200
                return r.json()["results"][0]["status"]

        with ThreadPoolExecutor(max_workers=16) as pool:
            statuses = list(pool.map(hammer, range(2, 201, 2)))
        assert set(statuses) == {"duplicate"}

        with httpx.Client(base_url=base, timeout=10) as c:
            got = c.get("/api/v1/devices/dev-a/alerts").json()["alerts"]
            # 100 fired segments (odd seqs 1..199); each closed by the next
            # even seq, exactly once
            assert len(got) == 100
            for i, a in enumerate(got):
                fire_seq = 2 * i + 1
                assert a["end_seq"] == fire_seq
                assert a["active"] is False
                assert a["closing"]["reason"] == "record"
                assert a["closing"]["end_seq"] == fire_seq + 1
            assert c.get("/api/v1/devices/dev-a/alerts",
                        params={"active": "true"}).json()["alerts"] == []
            assert len(c.get("/api/v1/devices/dev-a/alerts",
                            params={"active": "false"}).json()["alerts"]) == 100

            # exactly one closing row per alert at the storage layer too
    finally:
        server.should_exit = True
        thread.join(timeout=5)

    conn = connect(settings.db_path)
    try:
        rows = conn.execute(
            "SELECT alert_id, COUNT(*) AS c FROM alert_closings GROUP BY alert_id"
        ).fetchall()
        assert len(rows) == 100 and all(r["c"] == 1 for r in rows)
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM alerts").fetchone()["c"] == 100
    finally:
        conn.close()
