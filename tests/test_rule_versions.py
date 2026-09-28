"""Versioned alert rules: version switches, atomic per-seq selection, history.

Covers:
* version creation/listing and monotonic effective_seq validation;
* per-seq effective-version selection on direct commits and buffered drains;
* frozen historical versions and version-pinned alerts + version filters;
* 断档 interplay (live versions reset, superseded versions stay frozen);
* startup migration of pre-versioning SQLite databases;
* real-server concurrency: racing modifications and racing batch commits
  must pick the unique effective version atomically per record seq.
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
    return client.put(
        f"/api/v1/devices/{device}/alert-rules/{rule_id}", json=body
    )


def versions(client, rule_id, device="dev-a"):
    resp = client.get(
        f"/api/v1/devices/{device}/alert-rules/{rule_id}/versions"
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["versions"]


def alerts(client, device="dev-a", **params):
    resp = client.get(f"/api/v1/devices/{device}/alerts", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()["alerts"]


def spans(alert_list):
    return [(a["version_no"], a["start_seq"], a["end_seq"]) for a in alert_list]


# --------------------------------------------------------------------- #
# version management
# --------------------------------------------------------------------- #

def test_create_rule_starts_at_version_1_effective_zero(client):
    rule = create_rule(client, consecutive=3)
    assert rule["version_no"] == 1
    assert rule["rule_version_id"] == 1
    assert rule["effective_seq"] == 0
    assert rule["version_count"] == 1
    assert rule["state"] == {"last_seq": None, "streak": 0, "alerted": False}

    vs = versions(client, rule["rule_id"])
    assert len(vs) == 1
    assert vs[0]["version_no"] == 1
    assert vs[0]["effective_seq"] == 0


def test_modify_appends_incrementing_version(client):
    rule = create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 2])  # advance the device frontier to 3

    r = modify(client, rule["rule_id"],
               {"field": "temp", "op": "gt", "threshold": 50, "consecutive": 2})
    assert r.status_code == 200, r.text
    v2 = r.json()
    assert v2["version_no"] == 2
    assert v2["rule_version_id"] == 2
    assert v2["threshold"] == 50
    assert v2["consecutive"] == 2
    # default effective_seq is the device's current next_expected
    assert v2["effective_seq"] == 3
    assert v2["version_count"] == 2
    # a new version begins with a fresh streak
    assert v2["state"] == {"last_seq": None, "streak": 0, "alerted": False}

    # list reflects the current (newest) version
    listed = client.get("/api/v1/devices/dev-a/alert-rules").json()["rules"][0]
    assert listed["version_no"] == 2
    assert listed["threshold"] == 50

    vs = versions(client, rule["rule_id"])
    assert [v["version_no"] for v in vs] == [1, 2]
    assert [(v["threshold"], v["effective_seq"]) for v in vs] == [(30.0, 0), (50.0, 3)]


def test_explicit_effective_seq_must_be_strictly_greater(client):
    rule = create_rule(client, consecutive=1)
    submit(client, "dev-a", [1, 2, 3])
    # same seq as v1's effective_seq -> 409
    assert modify(client, rule["rule_id"],
                  {"field": "temp", "op": "gt", "threshold": 1,
                   "consecutive": 1, "effective_seq": 0}).status_code == 409
    # any non-monotonic seq -> 409
    assert modify(client, rule["rule_id"],
                  {"field": "temp", "op": "gt", "threshold": 1,
                   "consecutive": 1, "effective_seq": 5}).status_code == 200
    assert modify(client, rule["rule_id"],
                  {"field": "temp", "op": "gt", "threshold": 1,
                   "consecutive": 1, "effective_seq": 5}).status_code == 409
    assert modify(client, rule["rule_id"],
                  {"field": "temp", "op": "gt", "threshold": 1,
                   "consecutive": 1, "effective_seq": 4}).status_code == 409
    assert modify(client, rule["rule_id"],
                  {"field": "temp", "op": "gt", "threshold": 1,
                   "consecutive": 1, "effective_seq": 6}).status_code == 200
    # failed modifications appended nothing
    assert [v["version_no"] for v in versions(client, rule["rule_id"])] == [1, 2, 3]


def test_modify_validation_errors(client):
    rule = create_rule(client, consecutive=1)
    # unknown rule on this device
    r = modify(client, 999, {"field": "temp", "op": "gt", "threshold": 1,
                             "consecutive": 1})
    assert r.status_code == 404
    assert client.get(
        "/api/v1/devices/dev-a/alert-rules/999/versions"
    ).status_code == 404
    # body validation
    r = modify(client, rule["rule_id"],
               {"field": "temp", "op": "gte", "threshold": 1, "consecutive": 1})
    assert r.status_code == 422
    r = modify(client, rule["rule_id"],
               {"field": "temp", "op": "gt", "threshold": 1, "consecutive": 0,
                "effective_seq": 1})
    assert r.status_code == 422
    r = modify(client, rule["rule_id"],
               {"field": "temp", "op": "gt", "threshold": 1, "consecutive": 1,
                "effective_seq": -1})
    assert r.status_code == 422


def test_modify_is_device_scoped(client):
    rule = create_rule(client, device="dev-a", consecutive=1)
    # same rule id under another device is unknown
    r = modify(client, rule["rule_id"],
               {"field": "temp", "op": "gt", "threshold": 1, "consecutive": 1},
               device="dev-b")
    assert r.status_code == 404
    r = client.get("/api/v1/devices/dev-b/alert-rules/1/versions")
    assert r.status_code == 404


# --------------------------------------------------------------------- #
# atomic per-seq version selection
# --------------------------------------------------------------------- #

def test_direct_commits_select_version_by_seq(client):
    rule = create_rule(client, consecutive=1)
    # v2 from seq 4: threshold 999 -> violations stop; v3 from seq 7: thr 10
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 4})
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 10, "consecutive": 1,
            "effective_seq": 7})

    # v1 owns 1..3: violate, normal, violate -> two independent runs
    submit_payloads(client, "dev-a", [
        (1, {"temp": 40}), (2, {"temp": 5}), (3, {"temp": 40})])
    # v2 owns 4..6: 40 < 999 never violates
    submit(client, "dev-a", [4, 5, 6])
    # v3 owns 7..9: violate, normal, violate -> fires again independently
    submit_payloads(client, "dev-a", [
        (7, {"temp": 40}), (8, {"temp": 5}), (9, {"temp": 40})])
    assert spans(alerts(client)) == [
        (1, 1, 1), (1, 3, 3), (3, 7, 7), (3, 9, 9),
    ]

    vs = versions(client, rule["rule_id"])
    states = {v["version_no"]: v["state"] for v in vs}
    assert states[1] == {"last_seq": 3, "streak": 1, "alerted": True}
    assert states[2] == {"last_seq": 6, "streak": 0, "alerted": False}
    assert states[3] == {"last_seq": 9, "streak": 1, "alerted": True}


def test_buffered_drain_selects_version_at_each_record_seq(client):
    """A rule switched while records sit buffered: when they drain to
    committed, each one picks the version effective at *its own* seq,
    atomically, in seq order."""
    rule = create_rule(client, consecutive=2)
    # seq 1 commits; 4,5 buffer ahead of the pending gap 2..3
    submit(client, "dev-a", [1, 4, 5])
    assert alerts(client) == []

    # v2 takes over exactly at seq 4 with a threshold 40 cannot reach
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 999, "consecutive": 1,
            "effective_seq": 4})

    # committing 2 drains 2,3: seq1 already counted, so v1 streak 1,2,3 fires
    # the run 1..2 once it reaches two violations (consecutive=2); seq3 merely
    # extends that already-alerted run. Then 4,5 are evaluated under v2
    # (40 < 999 -> no alert). The single v1 alert is pinned at end_seq 2.
    submit(client, "dev-a", [2, 3])
    assert spans(alerts(client)) == [(1, 1, 2)]

    states = {v["version_no"]: v["state"] for v in versions(client, rule["rule_id"])}
    # v1 last evaluated seq 3 (it must not have evaluated 4/5)
    assert states[1]["last_seq"] == 3
    assert states[1]["streak"] == 3
    assert states[1]["alerted"] is True
    assert states[2]["last_seq"] == 5
    assert states[2]["streak"] == 0


def test_new_version_starts_fresh_streak_and_fires_independently(client):
    rule = create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 2, 3])  # v1 alert at 3
    submit(client, "dev-a", [4])        # same run: v1 alerted, no new alert
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 30, "consecutive": 2,
            "effective_seq": 5})
    # v2 streak starts from zero even though v1 was mid-run
    submit(client, "dev-a", [5])
    assert spans(alerts(client)) == [(1, 1, 3)]
    submit(client, "dev-a", [6])
    assert spans(alerts(client)) == [(1, 1, 3), (2, 5, 6)]


def test_history_is_immutable_after_later_modifications(client):
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # v1 alert
    before = alerts(client)[0]
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "lt", "threshold": 1, "consecutive": 5,
            "effective_seq": 3})
    submit(client, "dev-a", [3, 4], value=-5.0)  # v2: -5 < 1, needs 5 -> no alert yet
    modify(client, rule["rule_id"],
           {"field": "hum", "op": "gt", "threshold": 9, "consecutive": 1,
            "effective_seq": 5})
    submit_payloads(client, "dev-a", [(5, {"hum": 99})])  # v3 alert

    # the original alert is byte-for-byte unchanged: still v1 + v1 snapshot
    after = alerts(client, rule_id=rule["rule_id"], version_no=1)
    assert len(after) == 1
    assert after[0] == before
    assert after[0]["field"] == "temp"
    assert after[0]["op"] == "gt"

    # v1 version definition itself stays frozen
    v1 = versions(client, rule["rule_id"])[0]
    assert (v1["field"], v1["op"], v1["threshold"], v1["consecutive"]) == (
        "temp", "gt", 30.0, 2)
    assert [a["version_no"] for a in alerts(client)] == [1, 3]


# --------------------------------------------------------------------- #
# alert filtering by version
# --------------------------------------------------------------------- #

def test_alerts_filter_by_version(client):
    rule = create_rule(client, consecutive=1)
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 10, "consecutive": 1,
            "effective_seq": 5})
    # v1 owns 1..4: violate, normal, violate, normal (two runs)
    submit_payloads(client, "dev-a", [
        (1, {"temp": 40}), (2, {"temp": 5}),
        (3, {"temp": 40}), (4, {"temp": 5})])
    # v2 owns 5..7: violate, violate, normal -> one run fires once
    submit_payloads(client, "dev-a", [
        (5, {"temp": 40}), (6, {"temp": 40}), (7, {"temp": 5})])
    all_alerts = alerts(client)
    assert spans(all_alerts) == [
        (1, 1, 1), (1, 3, 3), (2, 5, 5),
    ]

    assert spans(alerts(client, rule_id=1, version_no=1)) == [
        (1, 1, 1), (1, 3, 3)]
    assert spans(alerts(client, rule_id=1, version_no=2)) == [(2, 5, 5)]
    # global rule_version_id filter needs no rule_id
    vid2 = all_alerts[2]["rule_version_id"]
    assert spans(alerts(client, rule_version_id=vid2)) == [(2, 5, 5)]
    # version composes with status
    assert alerts(client, rule_id=1, version_no=1, status="open")
    assert alerts(client, rule_id=1, version_no=1, status="acknowledged") == []
    # version_no without rule_id is ambiguous -> 400
    resp = client.get("/api/v1/devices/dev-a/alerts", params={"version_no": 1})
    assert resp.status_code == 400
    # invalid values
    assert client.get(
        "/api/v1/devices/dev-a/alerts", params={"version_no": 0}
    ).status_code == 422


# --------------------------------------------------------------------- #
# gap interplay
# --------------------------------------------------------------------- #

def test_gap_resets_live_versions_but_keeps_superseded_history(client):
    rule = create_rule(client, consecutive=2)
    # v2 owns seq >= 5, same threshold, still needs 2 in a row.
    modify(client, rule["rule_id"],
           {"field": "temp", "op": "gt", "threshold": 30, "consecutive": 2,
            "effective_seq": 5})

    # 1,2 commit -> v1 fires once (run 1..2); 4 buffers behind the hole at 3.
    submit(client, "dev-a", [1, 2, 4])
    assert spans(alerts(client)) == [(1, 1, 2)]
    time.sleep(0.3)
    client.get("/api/v1/devices/dev-a/window")  # declares gap [3,3], drains 4
    # v1's history is frozen at this point: it evaluated through 4 (one
    # violating record past the alerted run); the gap reset broke the run.
    v1_before = versions(client, rule["rule_id"])[0]["state"]
    assert v1_before["last_seq"] == 4
    assert v1_before["streak"] == 1
    assert v1_before["alerted"] is False  # the declared gap re-armed v1
    # the historical v1 alert survives untouched
    assert spans(alerts(client)) == [(1, 1, 2)]

    # 7 buffers behind the next hole 5..6; declaring it must reset the *live*
    # v2 streak to 0 (v2 had evaluated nothing yet), instead of inheriting any
    # streak across the declared gap.
    submit(client, "dev-a", [7])
    time.sleep(0.3)
    client.get("/api/v1/devices/dev-a/window")  # declares gap [5,6], drains 7

    # superseded v1 must not be rewritten by a later gap: it was already
    # fully out of the window covered by the new declaration
    v1_after = versions(client, rule["rule_id"])[0]["state"]
    assert v1_after == v1_before

    v2_state = {v["version_no"]: v["state"]
                for v in versions(client, rule["rule_id"])}[2]
    assert v2_state["last_seq"] == 7
    assert v2_state["streak"] == 1   # 7 alone starts a fresh run
    assert v2_state["alerted"] is False

    # gap fills never recompute
    submit(client, "dev-a", [5, 6])
    v2_state = {v["version_no"]: v["state"]
                for v in versions(client, rule["rule_id"])}[2]
    assert (v2_state["streak"], v2_state["alerted"]) == (1, False)
    # and still nothing touched v1
    assert versions(client, rule["rule_id"])[0]["state"] == v1_before

    # one more violating committed record completes the fresh v2 run -> an
    # alert attributed entirely to v2, not straddling pre-gap history
    submit(client, "dev-a", [8])
    assert spans(alerts(client)) == [(1, 1, 2), (2, 7, 8)]
    assert versions(client, rule["rule_id"])[0]["state"] == v1_before


# --------------------------------------------------------------------- #
# SQLite migration from a pre-versioning database
# --------------------------------------------------------------------- #

def test_pre_versioning_database_is_migrated(tmp_path):
    """Old wide alert_rules + rule_id-keyed state become rule series + v1."""
    import sqlite3

    from app.db import connect, init_db

    path = str(tmp_path / "old.db")
    with sqlite3.connect(path) as raw:
        raw.execute(
            "CREATE TABLE devices (device_id TEXT PRIMARY KEY, next_expected INTEGER, "
            "gap_since REAL, duplicates_count INTEGER, created_at REAL, updated_at REAL)"
        )
        raw.execute(
            "INSERT INTO devices VALUES ('dev-a', 4, NULL, 0, 1.0, 1.0)"
        )
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
        raw.execute("INSERT INTO alert_rule_state VALUES (1, 3, 2, 1, 2.0)")
        raw.execute(
            "CREATE TABLE alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id INTEGER, "
            "device_id TEXT, field TEXT, op TEXT, threshold REAL, consecutive INTEGER, "
            "start_seq INTEGER, end_seq INTEGER, trigger_value REAL, created_at REAL, "
            "status TEXT, acknowledged_by TEXT, acknowledged_at REAL)"
        )
        raw.execute(
            "INSERT INTO alerts (rule_id, device_id, field, op, threshold, consecutive, "
            "start_seq, end_seq, trigger_value, created_at, status) "
            "VALUES (1, 'dev-a', 'temp', 'gt', 30, 2, 2, 3, 40, 2.0, 'open')"
        )

    init_db(path)
    init_db(path)  # migration is idempotent

    conn = connect(path)
    try:
        v = conn.execute(
            "SELECT * FROM alert_rule_versions WHERE rule_id = 1"
        ).fetchone()
        assert (v["version_no"], v["effective_seq"], v["field"],
                v["threshold"], v["consecutive"]) == (1, 0, "temp", 30.0, 2)
        st = conn.execute(
            "SELECT * FROM alert_rule_state WHERE version_id = ?", (v["id"],)
        ).fetchone()
        # prior streak/alerted carried over to v1
        assert (st["last_seq"], st["streak"], st["alerted"]) == (3, 2, 1)
        a = conn.execute("SELECT * FROM alerts WHERE id = 1").fetchone()
        assert (a["version_id"], a["version_no"]) == (v["id"], 1)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()

    # migrated rule keeps working through the API and reports a single v1
    settings = Settings(db_path=path, sweep_enabled=False)
    with TestClient(create_app(settings)) as c:
        body = c.get("/api/v1/devices/dev-a/alert-rules").json()
        assert body["rules"][0]["version_count"] == 1
        assert body["rules"][0]["version_no"] == 1
        vs = c.get("/api/v1/devices/dev-a/alert-rules/1/versions").json()
        assert len(vs["versions"]) == 1
        alert = c.get(
            "/api/v1/devices/dev-a/alerts",
            params={"rule_id": 1, "version_no": 1},
        ).json()["alerts"]
        assert len(alert) == 1 and alert[0]["version_no"] == 1
        # a new version can be appended on top of the migrated series
        r = c.put("/api/v1/devices/dev-a/alert-rules/1",
                  json={"field": "temp", "op": "gt", "threshold": 9,
                        "consecutive": 1, "effective_seq": 100})
        assert r.status_code == 200, r.text
        assert r.json()["version_no"] == 2


def test_migrated_orphan_alert_gets_placeholder_version(tmp_path):
    """An alert whose rule row was missing still gets pinned to a v1 series."""
    import sqlite3

    from app.db import init_db

    path = str(tmp_path / "orphan.db")
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
            "VALUES (7, 'dev-o', 'temp', 'gt', 30, 2, 1, 2, 40, 1.0)"
        )

    init_db(path)
    settings = Settings(db_path=path, sweep_enabled=False)
    with TestClient(create_app(settings)) as c:
        a = c.get("/api/v1/devices/dev-o/alerts").json()["alerts"]
        assert len(a) == 1
        assert a[0]["version_no"] == 1
        assert a[0]["rule_id"] == 7


# --------------------------------------------------------------------- #
# concurrency against a real uvicorn server
# --------------------------------------------------------------------- #

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Server:
    def __init__(self, settings, **uv_kwargs):
        import uvicorn

        self.port = _free_port()
        config = uvicorn.Config(
            create_app(settings), host="127.0.0.1", port=self.port,
            log_level="error", **uv_kwargs,
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    @property
    def base(self):
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self):
        self.thread.start()
        for _ in range(100):
            try:
                httpx.get(f"{self.base}/health", timeout=0.2)
                return self
            except Exception:
                time.sleep(0.05)
        pytest.fail("server did not start")

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(timeout=5)


def test_concurrent_modifications_serialize_to_unique_versions(tmp_path):
    """Many workers switch a rule at once: version_no is contiguous and each
    successful effective_seq is unique; same-seq attempts have one winner."""
    settings = Settings(
        db_path=str(tmp_path / "conc_mod.db"),
        window_size=10_000, gap_timeout_seconds=600, sweep_enabled=False,
    )
    with _Server(settings) as srv:
        with httpx.Client(base_url=srv.base, timeout=10) as c:
            r = c.post(
                "/api/v1/devices/dev-a/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30,
                      "consecutive": 2},
            )
            assert r.status_code == 201
            # move the frontier off 0 so distinct explicit seqs are possible
            r = c.post("/api/v1/batches", json={
                "device_id": "dev-a",
                "records": [{"seq": 1, "payload": {"temp": 1}}],
            })
            assert r.status_code == 200

        # distinct, increasing seqs committed in arbitrary order: successes
        # must form a strictly increasing chain with no version_no gaps.
        def switch(seq):
            with httpx.Client(base_url=srv.base, timeout=10) as c:
                r = c.put(
                    "/api/v1/devices/dev-a/alert-rules/1",
                    json={"field": "temp", "op": "gt", "threshold": float(seq),
                          "consecutive": 1, "effective_seq": seq},
                )
                return r.status_code

        with ThreadPoolExecutor(max_workers=12) as pool:
            codes = list(pool.map(switch, range(2, 26)))
        assert set(codes) <= {200, 409}

        with httpx.Client(base_url=srv.base, timeout=10) as c:
            vs = c.get(
                "/api/v1/devices/dev-a/alert-rules/1/versions"
            ).json()["versions"]
            seqs = [v["effective_seq"] for v in vs]
            nos = [v["version_no"] for v in vs]
            assert nos == list(range(1, len(vs) + 1))  # contiguous versioning
            assert len(seqs) == len(set(seqs))         # unique effective seqs
            assert seqs == sorted(seqs)                # monotonic
            assert sum(code == 200 for code in codes) == len(vs) - 1

        # concurrent switches at the SAME effective seq: exactly one wins
        barrier = threading.Barrier(16)

        def same_seq_switch(_):
            barrier.wait()
            with httpx.Client(base_url=srv.base, timeout=10) as c:
                return c.put(
                    "/api/v1/devices/dev-a/alert-rules/1",
                    json={"field": "temp", "op": "gt", "threshold": 7,
                          "consecutive": 1, "effective_seq": 1000},
                ).status_code

        with ThreadPoolExecutor(max_workers=16) as pool:
            codes = list(pool.map(same_seq_switch, range(16)))
        assert codes.count(200) == 1
        assert codes.count(409) == 15


def test_concurrent_batches_pick_one_version_per_seq(tmp_path):
    """8 threads shard seqs 2..200 while v1/v2/v3 own disjoint seq ranges.

    Whichever commit order the serialized writers choose, every record is
    evaluated in global ascending seq order (buffered drain) against exactly
    one version, and a version's streak starts fresh at its effective_seq.
    The payload pattern per seq is deterministic, so the expected alert ends
    are fixed regardless of how the threads interleave. Seq 1 is committed up
    front to anchor the window (avoiding the unrelated racing-first-batch
    rebase path) and is non-violating so it adds no alert.
    """
    settings = Settings(
        db_path=str(tmp_path / "conc_batch.db"),
        window_size=10_000, gap_timeout_seconds=600, sweep_enabled=False,
    )
    with _Server(settings) as srv:
        with httpx.Client(base_url=srv.base, timeout=10) as c:
            assert c.post(
                "/api/v1/devices/dev-c/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30,
                      "consecutive": 2},
            ).status_code == 201
            # v2 owns [50,149] needing 3 in a row; v3 owns [150,200] needing 1
            assert c.put(
                "/api/v1/devices/dev-c/alert-rules/1",
                json={"field": "temp", "op": "gt", "threshold": 30,
                      "consecutive": 3, "effective_seq": 50},
            ).status_code == 200
            assert c.put(
                "/api/v1/devices/dev-c/alert-rules/1",
                json={"field": "temp", "op": "gt", "threshold": 30,
                      "consecutive": 1, "effective_seq": 150},
            ).status_code == 200
            # anchor the window before the threads race
            r = c.post("/api/v1/batches", json={
                "device_id": "dev-c",
                "records": [{"seq": 1, "payload": {"temp": 5}}],
            })
            assert r.status_code == 200

        total, threads = 200, 8

        # Violation pattern, aligned to every version boundary:
        #   [2,49]   pairs land on (2,3),(5,6),... (suits consecutive = 2)
        #   [50,149] period 4 -> VVVn (triples, suits consecutive = 3)
        #   [150,200] odd seqs only (suits consecutive = 1)
        def violates(s):
            if s == 1:
                return False
            if s < 50:
                return s % 3 in (2, 0)
            if s < 150:
                return s % 4 in (1, 2, 3)
            return s % 2 == 1

        def worker(shard):
            lo = 2 + shard * 25
            hi = lo + 24 if shard < 7 else 200  # 8 shards cover 2..200
            with httpx.Client(base_url=srv.base, timeout=10) as c:
                start = lo
                while start <= hi:
                    seqs = list(range(start, min(start + 5, hi + 1)))
                    r = c.post("/api/v1/batches", json={
                        "device_id": "dev-c",
                        "records": [
                            {"seq": s, "payload": {"temp": 40 if violates(s) else 5}}
                            for s in seqs
                        ],
                    })
                    assert r.status_code == 200, r.text
                    start += 5

        with ThreadPoolExecutor(max_workers=threads) as pool:
            list(pool.map(worker, range(threads)))

        with httpx.Client(base_url=srv.base, timeout=10) as c:
            all_alerts = c.get("/api/v1/devices/dev-c/alerts").json()["alerts"]
            win = c.get("/api/v1/devices/dev-c/window").json()
            assert win["next_expected"] == 201
            assert win["stats"]["committed"] == total

            by_version = {1: [], 2: [], 3: []}
            for a in all_alerts:
                by_version[a["version_no"]].append(a)
                # streak accounting per version: a run can never include
                # records from before the version took effect
                assert a["start_seq"] >= {1: 0, 2: 50, 3: 150}[a["version_no"]]

            v1_ends = sorted(a["end_seq"] for a in by_version[1])
            v2_ends = sorted(a["end_seq"] for a in by_version[2])
            v3_ends = sorted(a["end_seq"] for a in by_version[3])
            # v1 consecutive=2 on (2,3),(5,6),...,(47,48) -> ends 3,6,...,48
            assert v1_ends == list(range(3, 49, 3))
            # v2 fresh streak consecutive=3 on VVVn in [50,149] -> 55,59,...,147
            assert v2_ends == list(range(55, 150, 4))
            # v3 consecutive=1 on odd seqs in [150,200] -> 151,153,...,199
            assert v3_ends == list(range(151, 201, 2))

            # every record seq maps to exactly one version; filters agree
            for no, ends in ((1, v1_ends), (2, v2_ends), (3, v3_ends)):
                got = c.get(
                    "/api/v1/devices/dev-c/alerts",
                    params={"rule_id": 1, "version_no": no},
                ).json()["alerts"]
                assert sorted(a["end_seq"] for a in got) == ends

            # version states partition the axis without overlap
            vs = c.get(
                "/api/v1/devices/dev-c/alert-rules/1/versions"
            ).json()["versions"]
            last = {v["version_no"]: v["state"]["last_seq"] for v in vs}
            assert last == {1: 49, 2: 149, 3: 200}


def test_concurrent_switch_and_batches_keep_alerts_consistent(tmp_path):
    """A rule switch racing direct commits: each seq still resolves to one
    unique version and no alert ever straddles a version boundary.

    One thread commits seq 1..300 while five threads keep appending versions
    (defaulting effective_seq to the moving frontier). Whichever wins each
    serialized write transaction, every committed seq has exactly one
    effective version and version_no stays contiguous.
    """
    settings = Settings(
        db_path=str(tmp_path / "conc_race.db"),
        window_size=10_000, gap_timeout_seconds=600, sweep_enabled=False,
    )
    with _Server(settings) as srv:
        with httpx.Client(base_url=srv.base, timeout=10) as c:
            assert c.post(
                "/api/v1/devices/dev-r/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30,
                      "consecutive": 1},
            ).status_code == 201

        stop = threading.Event()
        codes = []
        codes_lock = threading.Lock()

        def committer():
            for seq in range(1, 301):
                with httpx.Client(base_url=srv.base, timeout=10) as c:
                    r = c.post("/api/v1/batches", json={
                        "device_id": "dev-r",
                        "records": [{"seq": seq, "payload": {"temp": 40}}],
                    })
                    assert r.status_code == 200, r.text
                    if seq % 50 == 0:
                        time.sleep(0.002)  # give the switchers room to interleave

        def switcher(worker):
            i = 0
            with httpx.Client(base_url=srv.base, timeout=10) as c:
                while not stop.is_set():
                    # No explicit effective_seq: each switch targets the
                    # current commit frontier, which the BEGIN IMMEDIATE
                    # transactions pin atomically.
                    r = c.put("/api/v1/devices/dev-r/alert-rules/1", json={
                        "field": "temp", "op": "gt",
                        "threshold": float(30 + worker * 7 + i % 3),
                        "consecutive": 1,
                    })
                    with codes_lock:
                        codes.append(r.status_code)
                    assert r.status_code in (200, 409)
                    i += 1
                    time.sleep(0.001)

        with ThreadPoolExecutor(max_workers=6) as pool:
            sws = [pool.submit(switcher, w) for w in range(5)]
            cm = pool.submit(committer)
            cm.result()
            stop.set()
            for f in sws:
                f.result()

        with httpx.Client(base_url=srv.base, timeout=10) as c:
            all_alerts = c.get("/api/v1/devices/dev-r/alerts").json()["alerts"]
            win = c.get("/api/v1/devices/dev-r/window").json()
            assert win["next_expected"] == 301
            vs = c.get(
                "/api/v1/devices/dev-r/alert-rules/1/versions"
            ).json()["versions"]
            bounds = sorted((v["effective_seq"], v["version_no"]) for v in vs)

            # contiguous version numbering, strictly increasing effective seqs
            assert [v["version_no"] for v in vs] == list(range(1, len(vs) + 1))
            effs = [eff for eff, _ in bounds]
            assert effs == sorted(set(effs))
            # the racing switches really interleaved with the commits
            assert 200 in codes

            def owner(seq):
                owner_no = 1
                for eff, no in bounds:
                    if eff <= seq:
                        owner_no = no
                return owner_no

            ends = sorted(a["end_seq"] for a in all_alerts)
            assert len(ends) == len(set(ends))  # exactly one alert per seq
            for a in all_alerts:
                # each alert belongs to the unique version effective at its seq
                assert a["version_no"] == owner(a["end_seq"])
                # an alert run never straddles its version's start boundary
                assert a["start_seq"] >= next(
                    eff for eff, no in bounds if no == a["version_no"]
                )
