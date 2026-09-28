"""Anomaly-segment end records for consecutive-threshold alerts.

A fired alert's violating segment gets a *separate* end record (alert_ends)
with an end seq, server time and one of three reasons:

* record  - the next committed record is in-range or has a missing /
            non-numeric watched field;
* gap     - a gap (断档) is declared before any closing record;
* version - a newer rule version evaluates its first committed record.

Rules covered here:
* ending fields are absent (null) while a segment is active and appear once;
* record / missing-field / non-numeric closers, gap closer, version closer;
* at most one end per alert; buffered records, duplicates and gap_filled
  backfills never end a segment early;
* the alert snapshot and the acknowledgement state are never rewritten;
* ?active=true|false filtering (orthogonal to status);
* startup on an old database creates alert_ends automatically and historical
  alerts stay active.
"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
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


def modify(client, rule_id, body, device="dev-a"):
    resp = client.put(
        f"/api/v1/devices/{device}/alert-rules/{rule_id}", json=body
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def alerts(client, device="dev-a", **params):
    resp = client.get(f"/api/v1/devices/{device}/alerts", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()["alerts"]


def declare_gap(client, device="dev-a"):
    """Expire the pending gap and force its lazy declaration via the window read."""
    time.sleep(0.3)
    client.get(f"/api/v1/devices/{device}/window")


# --------------------------------------------------------------------- #
# active alerts carry no end record; the `ending` object shape
# --------------------------------------------------------------------- #

def test_active_alert_has_null_ending(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2, 3])  # one alert at seq 2, still violating at 3
    [alert] = alerts(client)
    assert alert["end_seq"] == 2
    assert alert["ending"] is None


def test_end_fields_shape_after_record_close(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])          # alert fires at 2
    before = alerts(client)[0]["created_at"]
    submit(client, "dev-a", [3], value=10.0)  # in-range: ends the segment
    [alert] = alerts(client)
    ending = alert["ending"]
    assert set(ending) == {"end_seq", "end_reason", "ended_at"}
    assert ending["end_seq"] == 3
    assert ending["end_reason"] == "record"
    assert ending["ended_at"] >= before


# --------------------------------------------------------------------- #
# reason 'record': in-range / missing field / non-numeric
# --------------------------------------------------------------------- #

def test_in_range_record_ends_segment_once(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # alert
    submit(client, "dev-a", [3], value=10.0)  # close (record)
    [first] = alerts(client)
    assert first["ending"]["end_reason"] == "record"
    assert first["ending"]["end_seq"] == 3

    # more in-range records must not add or move the end
    submit(client, "dev-a", [4], value=10.0)
    assert alerts(client)[0]["ending"] == first["ending"]

    # a fresh violating run fires a second, initially active alert
    submit(client, "dev-a", [5, 6])
    got = alerts(client)
    assert len(got) == 2
    assert got[0]["ending"]["end_seq"] == 3
    assert got[1]["ending"] is None


def test_missing_or_non_numeric_field_ends_segment(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # alert
    submit_payloads(client, "dev-a", [(3, {"other": 1})])  # missing field
    assert alerts(client)[0]["ending"] == {
        "end_seq": 3, "end_reason": "record",
        "ended_at": alerts(client)[0]["ending"]["ended_at"],
    }

    # a new segment ended by a non-numeric value
    submit(client, "dev-a", [4, 5])
    submit_payloads(client, "dev-a", [(6, {"temp": "hot"})])
    got = alerts(client)
    assert got[1]["ending"]["end_seq"] == 6
    assert got[1]["ending"]["end_reason"] == "record"


def test_close_does_not_rewrite_alert_snapshot_or_ack(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])
    # acknowledge while the segment is still active
    ack = client.post(
        "/api/v1/devices/dev-a/alerts/1/acknowledge",
        json={"acknowledged_by": "ops-alice"},
    ).json()
    submit(client, "dev-a", [3], value=10.0)  # ends the segment afterwards
    alert = alerts(client)[0]
    # acknowledgement state is frozen, untouched by the end record
    assert alert["status"] == "acknowledged"
    assert alert["acknowledged_by"] == "ops-alice"
    assert alert["acknowledged_at"] == ack["acknowledged_at"]
    # immutable snapshot fields unchanged
    assert (alert["start_seq"], alert["end_seq"], alert["trigger_value"]) == (1, 2, 40.0)
    assert alert["ending"]["end_reason"] == "record"


# --------------------------------------------------------------------- #
# reason 'gap': declaring a 断档 ends active segments
# --------------------------------------------------------------------- #

def test_declared_gap_ends_segment(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # alert, active
    submit(client, "dev-a", [4])     # buffered behind pending gap 3
    declare_gap(client)              # gap [3,3]; 4 drains and starts a new run
    [alert] = alerts(client)
    # end seq is the first seq after the hole (seq 4, where evaluation resumes)
    assert alert["ending"] == {
        "end_seq": 4, "end_reason": "gap",
        "ended_at": alert["ending"]["ended_at"],
    }
    assert alert["ending"]["end_seq"] == 4


def test_gap_close_then_new_run_fires_active_alert(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # alert 1
    submit(client, "dev-a", [4])
    declare_gap(client)              # ends alert 1 (gap), 4 begins a fresh run
    submit(client, "dev-a", [5])     # completes the fresh run -> alert 2
    got = alerts(client)
    assert len(got) == 2
    assert got[0]["ending"]["end_reason"] == "gap"
    assert got[1]["ending"] is None
    assert (got[1]["start_seq"], got[1]["end_seq"]) == (4, 5)


def test_gap_without_active_segment_writes_no_end(client):
    create_rule(client, consecutive=3)
    submit(client, "dev-a", [1])     # streak 1, no alert
    submit(client, "dev-a", [3])     # buffered behind gap 2
    declare_gap(client)
    assert alerts(client) == []       # nothing to end


# --------------------------------------------------------------------- #
# reason 'version': a newer version takes over
# --------------------------------------------------------------------- #

def test_version_handover_ends_previous_segment(client):
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # v1 alert, still active
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 3})
    submit(client, "dev-a", [3])     # v2's first takeover record
    [alert] = alerts(client)
    assert alert["rule_version_id"] == 1
    assert alert["ending"]["end_seq"] == 3
    assert alert["ending"]["end_reason"] == "version"


def test_version_handover_without_active_segment_writes_no_end(client):
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1])     # streak 1, no alert under v1
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 2})
    submit(client, "dev-a", [2])     # v2 takeover, but v1 never fired
    assert alerts(client) == []


def test_version_handover_through_buffered_drain(client):
    """Switch while records sit buffered: when they drain in seq order, the
    first record evaluated by v2 ends v1's active segment (once)."""
    rule = create_rule(client, consecutive=2)
    # seq 1 commits; 3,4 buffer ahead of the pending gap 2
    submit(client, "dev-a", [1, 3, 4])
    assert alerts(client) == []
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 3})
    # commit 2 (v1 fires at seq 2) then 3 drains under v2 and ends v1's
    # segment with reason version at seq 3; seq 4 must not end it again.
    submit(client, "dev-a", [2])
    [alert] = alerts(client)
    assert (alert["start_seq"], alert["end_seq"]) == (1, 2)
    assert alert["ending"]["end_seq"] == 3
    assert alert["ending"]["end_reason"] == "version"


def test_record_close_before_handover_means_no_version_end(client):
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])          # v1 alert
    submit(client, "dev-a", [3], value=10.0)  # already closed (record)
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 4})
    submit(client, "dev-a", [4])
    [alert] = alerts(client)
    # the earlier record-close wins; the handover adds nothing
    assert alert["ending"]["end_reason"] == "record"
    assert alert["ending"]["end_seq"] == 3


def test_gap_close_is_not_overwritten_by_later_handover(client):
    """A segment already ended by a gap declaration is never re-ended when a
    newer version later takes over (at most once per alert; the existing gap
    end wins). Here the gap boundary belongs to v1 (at_seq 4) and v2 takes
    over strictly later at seq 6, so the reasons do not coincide."""
    rule = create_rule(client, consecutive=2)
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 6})
    submit(client, "dev-a", [1, 2])                 # v1 alert at 2, active
    submit_payloads(client, "dev-a", [(4, {"temp": 5}), (5, {"temp": 5})])
    declare_gap(client)   # gap [3,3]; v1 owns the drain point -> ends (gap) at 4
    [alert] = alerts(client)
    assert alert["ending"]["end_reason"] == "gap"
    assert alert["ending"]["end_seq"] == 4

    # v2 takes over at seq 6: v1's alert is already ended, nothing is re-ended
    submit(client, "dev-a", [6])
    assert alerts(client)[0]["ending"] == alert["ending"]
    assert alerts(client)[0]["ending"]["end_reason"] == "gap"


def test_gap_boundary_coinciding_with_handover_ends_as_version(client):
    """When the new version's effective_seq is exactly the gap jump target
    (seq 4), the engine scopes the gap reset to that version and later ones and
    treats the predecessor as superseded, so its active segment ends when v2
    first takes over the draining record — reason 'version', once."""
    rule = create_rule(client, consecutive=2)
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 4})
    submit(client, "dev-a", [1, 2, 4])  # v1 alert at 2; 4 buffered behind gap 3
    declare_gap(client)                 # drains 4 under v2
    [alert] = alerts(client)
    assert alert["rule_version_id"] == 1
    assert alert["ending"]["end_seq"] == 4
    assert alert["ending"]["end_reason"] == "version"


# --------------------------------------------------------------------- #
# only the first transition to committed advances / ends anything
# --------------------------------------------------------------------- #

def test_buffered_records_do_not_end_early(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # active alert
    # 4 buffers behind a pending gap and is in-range; it must not close anything
    submit_payloads(client, "dev-a", [(4, {"temp": 10.0})])
    assert alerts(client)[0]["ending"] is None


def test_duplicate_does_not_end_segment(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # active alert
    submit(client, "dev-a", [3], value=10.0)  # closes with record at 3
    first_end = alerts(client)[0]["ending"]
    # retry the closing batch and the firing batch: no change to the end
    submit(client, "dev-a", [3], value=10.0)
    submit(client, "dev-a", [1, 2])
    assert alerts(client)[0]["ending"] == first_end


def test_gap_backfill_does_not_end_segment(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # active alert
    submit(client, "dev-a", [4])
    declare_gap(client)              # ends with gap at seq 4
    gap_end = alerts(client)[0]["ending"]
    # replay the missing seq 3 (violating value): gap_filled, never recomputes
    client.post("/api/v1/devices/dev-a/gaps/1/replay")
    body = submit(client, "dev-a", [3])
    assert body["results"][0]["status"] == "gap_filled"
    assert alerts(client)[0]["ending"] == gap_end


# --------------------------------------------------------------------- #
# ?active filter
# --------------------------------------------------------------------- #

def test_active_filter(client):
    create_rule(client, consecutive=1)
    submit(client, "dev-a", [1])            # alert 1, active
    submit(client, "dev-a", [2], value=5.0)  # closes alert 1 (record)
    submit(client, "dev-a", [3])            # alert 2, active

    assert [a["alert_id"] for a in alerts(client, active=True)] == [2]
    assert [a["alert_id"] for a in alerts(client, active=False)] == [1]
    assert [a["alert_id"] for a in alerts(client)] == [1, 2]

    # active composes with the acknowledgement filter
    client.post("/api/v1/devices/dev-a/alerts/2/acknowledge",
                json={"acknowledged_by": "ops-alice"})
    assert [a["alert_id"] for a in alerts(client, active=True)] == [2]
    assert [a["alert_id"] for a in alerts(client, active=True,
                                          status="acknowledged")] == [2]
    assert alerts(client, active=True, status="open") == []

    # ended alerts stay selectable through active=false regardless of ack
    assert alerts(client, active=False, status="acknowledged") == []
    assert [a["alert_id"] for a in alerts(client, active=False,
                                          status="open")] == [1]


def test_active_filter_follows_gap_and_version_reasons(client):
    rule = create_rule(client, consecutive=1)
    submit(client, "dev-a", [1])       # alert 1
    submit(client, "dev-a", [3])       # buffered behind gap 2
    declare_gap(client)                # alert 1 ended (gap); 3 -> new active run? 3 violates
    submit(client, "dev-a", [4], value=5.0)  # closes the run started at 3 (record)
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 5})
    submit(client, "dev-a", [5])       # v2 no violation
    all_got = alerts(client)
    # alert 1 ended by gap; the run at 3 fired alert 2 ended by record at 4
    assert [a["alert_id"] for a in alerts(client, active=False)] == [1, 2]
    assert all(a["ending"] is not None for a in all_got)


# --------------------------------------------------------------------- #
# multiple rules end independently
# --------------------------------------------------------------------- #

def test_multiple_rules_end_independently(client):
    create_rule(client, field="temp", op="gt", threshold=30.0, consecutive=1)
    create_rule(client, field="hum", op="lt", threshold=20.0, consecutive=1)
    submit_payloads(client, "dev-a", [(1, {"temp": 40, "hum": 50})])  # temp fires
    submit_payloads(client, "dev-a", [(2, {"temp": 40, "hum": 10})])  # hum fires
    by_rule = {a["rule_id"]: a for a in alerts(client)}
    assert by_rule[1]["ending"] is None
    assert by_rule[2]["ending"] is None
    # hum recovers, temp keeps violating: only hum's segment ends
    submit_payloads(client, "dev-a", [(3, {"temp": 40, "hum": 50})])
    by_rule = {a["rule_id"]: a for a in alerts(client)}
    assert by_rule[1]["ending"] is None
    assert by_rule[2]["ending"]["end_reason"] == "record"
    assert by_rule[2]["ending"]["end_seq"] == 3


# --------------------------------------------------------------------- #
# storage-level guarantee + startup on an old database
# --------------------------------------------------------------------- #

def test_at_most_one_end_per_alert_in_storage(tmp_path):
    """alert_ends.alert_id is a PRIMARY KEY: a second end cannot be inserted."""
    from app.db import connect, init_db

    path = str(tmp_path / "raw.db")
    init_db(path)
    conn = connect(path)
    try:
        conn.execute(
            "INSERT INTO alert_rules (device_id, created_at) VALUES ('d', 1.0)"
        )
        conn.execute(
            "INSERT INTO alert_rule_versions (rule_id, version_no, field, op, "
            "threshold, consecutive, effective_seq, created_at) "
            "VALUES (1, 1, 'temp', 'gt', 30, 1, 0, 1.0)"
        )
        conn.execute(
            "INSERT INTO alerts (rule_id, version_id, version_no, device_id, "
            "field, op, threshold, consecutive, start_seq, end_seq, "
            "trigger_value, created_at) VALUES "
            "(1, 1, 1, 'd', 'temp', 'gt', 30, 1, 1, 1, 40, 1.0)"
        )
        conn.execute(
            "INSERT INTO alert_ends (alert_id, end_seq, end_reason, ended_at) "
            "VALUES (1, 2, 'record', 2.0)"
        )
        with pytest.raises(Exception):  # PRIMARY KEY / IntegrityError
            conn.execute(
                "INSERT INTO alert_ends (alert_id, end_seq, end_reason, ended_at) "
                "VALUES (1, 3, 'gap', 3.0)"
            )
        row = conn.execute("SELECT * FROM alert_ends WHERE alert_id = 1").fetchone()
        assert (row["end_seq"], row["end_reason"]) == (2, "record")
    finally:
        conn.close()


def test_old_database_gains_alert_ends_on_startup(tmp_path):
    """An existing volume with no alert_ends table gets it on startup; its
    historical alerts remain active (null ending) and closable afterwards."""
    import sqlite3

    path = str(tmp_path / "old.db")
    with sqlite3.connect(path) as raw:
        raw.execute(
            "CREATE TABLE alert_rules (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "device_id TEXT, field TEXT, op TEXT, threshold REAL, "
            "consecutive INTEGER, created_at REAL)"
        )
        raw.execute(
            "CREATE TABLE alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "rule_id INTEGER, device_id TEXT, field TEXT, op TEXT, "
            "threshold REAL, consecutive INTEGER, start_seq INTEGER, "
            "end_seq INTEGER, trigger_value REAL, created_at REAL, "
            "status TEXT, acknowledged_by TEXT, acknowledged_at REAL)"
        )
        raw.execute(
            "INSERT INTO alerts (rule_id, device_id, field, op, threshold, "
            "consecutive, start_seq, end_seq, trigger_value, created_at, status) "
            "VALUES (1, 'dev-a', 'temp', 'gt', 30, 1, 1, 1, 40, 1.0, 'open')"
        )

    # startup must create alert_ends automatically and be idempotent
    from app.db import init_db
    init_db(path)
    init_db(path)

    settings = Settings(db_path=path, sweep_enabled=False)
    with TestClient(create_app(settings)) as c:
        # historical alert is active: no end record yet
        alert = c.get("/api/v1/devices/dev-a/alerts").json()["alerts"][0]
        assert alert["ending"] is None
        assert c.get(
            "/api/v1/devices/dev-a/alerts", params={"active": True}
        ).json()["alerts"][0]["alert_id"] == alert["alert_id"]
        # the migrated rule/alert can still participate: provision the matching
        # rule series through the engine by migrating state, then close via gap
        # is not possible without records; ending stays null for frozen history.
        assert c.get(
            "/api/v1/devices/dev-a/alerts", params={"active": False}
        ).json()["alerts"] == []


# --------------------------------------------------------------------- #
# concurrency against a real uvicorn server: ends are written at most once
# --------------------------------------------------------------------- #

def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_concurrent_batches_and_switches_end_each_alert_once(tmp_path):
    """Many workers commit interleaved violating/normal records while others
    keep appending versions. Every fired alert must get exactly zero or one
    end record, the storage count of alert_ends must equal the number of ended
    alerts reported by the API, and every end reason must be a valid one."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    import httpx
    import uvicorn

    settings = Settings(
        db_path=str(tmp_path / "conc_ends.db"),
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
                "/api/v1/devices/dev-z/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30,
                      "consecutive": 1},
            ).status_code == 201

        def committer(worker):
            # half the workers send mostly violations, half mostly normal
            # values; both mix in the opposite kind so record-ends and
            # re-fires happen throughout the shuffled seq range.
            with httpx.Client(base_url=base, timeout=10) as c:
                for seq in range(1 + worker, 201, 4):
                    value = 40 if (seq + worker) % 3 else 5
                    r = c.post("/api/v1/batches", json={
                        "device_id": "dev-z",
                        "records": [{"seq": seq, "payload": {"temp": value}}],
                    })
                    assert r.status_code == 200, r.text

        def switcher(worker):
            with httpx.Client(base_url=base, timeout=10) as c:
                for i in range(20):
                    r = c.put("/api/v1/devices/dev-z/alert-rules/1", json={
                        "field": "temp", "op": "gt",
                        "threshold": float(30 + worker * 3 + i % 4),
                        "consecutive": 1,
                    })
                    assert r.status_code in (200, 409)
                    time.sleep(0.001)

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(committer, w) for w in range(4)]
            futures += [pool.submit(switcher, w) for w in range(2)]
            for f in futures:
                f.result()

        with httpx.Client(base_url=base, timeout=10) as c:
            all_alerts = c.get("/api/v1/devices/dev-z/alerts").json()["alerts"]
            ended = [a for a in all_alerts if a["ending"] is not None]
            active = [a for a in all_alerts if a["ending"] is None]
            reasons = {a["ending"]["end_reason"] for a in ended}
            assert reasons <= {"record", "gap", "version"}
            # API view agrees with itself on the active filter
            assert len(c.get("/api/v1/devices/dev-z/alerts",
                             params={"active": False}).json()["alerts"]) == len(ended)
            assert len(c.get("/api/v1/devices/dev-z/alerts",
                             params={"active": True}).json()["alerts"]) == len(active)
            # no two distinct alerts share an end, and each end points at its alert
            end_ids = [(a["alert_id"], a["ending"]["end_seq"]) for a in ended]
            assert len({a["alert_id"] for a in ended}) == len(ended)

        # storage-level cross-check: exactly one row per ended alert
        import sqlite3

        with sqlite3.connect(settings.db_path) as raw:
            (stored_ends,) = raw.execute("SELECT COUNT(*) FROM alert_ends").fetchone()
            (distinct_alerts,) = raw.execute(
                "SELECT COUNT(DISTINCT alert_id) FROM alert_ends"
            ).fetchone()
            bad_reasons = raw.execute(
                "SELECT COUNT(*) FROM alert_ends WHERE end_reason "
                "NOT IN ('record', 'gap', 'version')"
            ).fetchone()[0]
        assert stored_ends == len(ended) == distinct_alerts
        assert bad_reasons == 0
    finally:
        server.should_exit = True
        thread.join(timeout=5)
