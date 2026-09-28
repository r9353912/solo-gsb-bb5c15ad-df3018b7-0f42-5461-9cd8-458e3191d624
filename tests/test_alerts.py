"""Consecutive-threshold alerting: rule CRUD, streak semantics, gap interplay."""
from __future__ import annotations

import socket
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        window_size=50,
        gap_timeout_seconds=0.25,
        sweep_enabled=False,  # deterministic: gaps are declared lazily on access
    )
    with TestClient(create_app(settings)) as c:
        yield c


def submit(client, device, seqs, value=40.0, field="temp"):
    """Submit records whose payload is {field: value} (default violates temp>30)."""
    return submit_payloads(client, device, [(s, {field: value}) for s in seqs])


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


def create_rule(client, device="dev-a", field="temp", op="gt", threshold=30.0, consecutive=3):
    resp = client.post(
        f"/api/v1/devices/{device}/alert-rules",
        json={"field": field, "op": op, "threshold": threshold, "consecutive": consecutive},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def alerts(client, device="dev-a", **params):
    resp = client.get(f"/api/v1/devices/{device}/alerts", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()["alerts"]


def rule_state(client, device="dev-a", rule_index=0):
    body = client.get(f"/api/v1/devices/{device}/alert-rules").json()
    return body["rules"][rule_index]["state"]


def spans(alert_list):
    return [(a["start_seq"], a["end_seq"]) for a in alert_list]


# --------------------------------------------------------------------- #
# rule creation & listing
# --------------------------------------------------------------------- #

def test_create_and_list_rules(client):
    rule = create_rule(client, field="temp", op="gt", threshold=30.0, consecutive=3)
    assert rule["rule_id"] == 1
    assert rule["device_id"] == "dev-a"
    assert rule["state"] == {"last_seq": None, "streak": 0, "alerted": False}

    create_rule(client, field="hum", op="lt", threshold=10.0, consecutive=2)
    body = client.get("/api/v1/devices/dev-a/alert-rules").json()
    assert body["device_id"] == "dev-a"
    assert [(r["field"], r["op"], r["threshold"], r["consecutive"]) for r in body["rules"]] == [
        ("temp", "gt", 30.0, 3),
        ("hum", "lt", 10.0, 2),
    ]
    # rules are device-scoped
    assert client.get("/api/v1/devices/other/alert-rules").json()["rules"] == []


def test_rule_validation(client):
    for bad in (
        {"field": "", "op": "gt", "threshold": 1.0, "consecutive": 1},
        {"field": "temp", "op": "gte", "threshold": 1.0, "consecutive": 1},
        {"field": "temp", "op": "gt", "threshold": 1.0, "consecutive": 0},
        {"field": "temp", "op": "gt", "threshold": "hot", "consecutive": 1},
        {"field": "temp", "op": "gt", "threshold": 1.0, "consecutive": "three"},
    ):
        resp = client.post("/api/v1/devices/dev-a/alert-rules", json=bad)
        assert resp.status_code == 422, bad


def test_non_finite_threshold_rejected(client):
    resp = client.post(
        "/api/v1/devices/dev-a/alert-rules",
        content='{"field": "temp", "op": "gt", "threshold": NaN, "consecutive": 2}',
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 422


def test_rule_for_unknown_device_allowed(client):
    # Devices are created lazily by their first batch; rules may be
    # pre-provisioned and evaluate records committed from then on.
    create_rule(client, device="dev-future", consecutive=3)
    submit(client, "dev-future", [1, 2, 3])
    assert len(alerts(client, "dev-future")) == 1


# --------------------------------------------------------------------- #
# streak semantics
# --------------------------------------------------------------------- #

def test_alert_fires_once_per_violating_run(client):
    create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 2, 3, 4, 5])  # all violate (40 > 30)
    got = alerts(client)
    assert len(got) == 1
    alert = got[0]
    assert (alert["start_seq"], alert["end_seq"]) == (1, 3)
    assert alert["trigger_value"] == 40.0
    assert alert["op"] == "gt" and alert["threshold"] == 30.0
    assert alert["field"] == "temp" and alert["consecutive"] == 3
    # the run continues but does not fire again
    state = rule_state(client)
    assert state == {"last_seq": 5, "streak": 5, "alerted": True}


def test_retrigger_only_after_recovery(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])          # alert fires at 2
    submit(client, "dev-a", [3], value=10.0)  # recovers (10 <= 30)
    assert rule_state(client) == {"last_seq": 3, "streak": 0, "alerted": False}
    submit(client, "dev-a", [4, 5])          # new run -> second alert at 5
    assert spans(alerts(client)) == [(1, 2), (4, 5)]


def test_lt_operator(client):
    create_rule(client, op="lt", threshold=5.0, consecutive=2)
    submit(client, "dev-a", [1], value=3.0)
    submit(client, "dev-a", [2], value=7.0)   # not below threshold
    submit(client, "dev-a", [3, 4], value=1.0)
    assert spans(alerts(client)) == [(3, 4)]


def test_missing_or_non_numeric_field_breaks_streak(client):
    create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 2])  # streak 2
    submit_payloads(client, "dev-a", [
        (3, {"other": 1}),          # field missing
        (4, {"temp": "hot"}),       # non-numeric
        (5, {"temp": None}),        # null
        (6, {"temp": True}),        # bool is not a measurement
    ])
    # NaN can never violate (strict JSON clients cannot emit it, so post raw)
    resp = client.post(
        "/api/v1/batches",
        content='{"device_id": "dev-a", "records": [{"seq": 7, "payload": {"temp": NaN}}]}',
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200, resp.text
    submit(client, "dev-a", [8, 9, 10])
    assert spans(alerts(client)) == [(8, 10)]


def test_consecutive_one_alerts_on_first_violation(client):
    create_rule(client, consecutive=1)
    submit(client, "dev-a", [1], value=10.0)  # normal, no alert
    submit(client, "dev-a", [2])
    assert spans(alerts(client)) == [(2, 2)]


def test_multiple_rules_evaluate_independently(client):
    create_rule(client, field="temp", op="gt", threshold=30.0, consecutive=2)
    create_rule(client, field="hum", op="lt", threshold=20.0, consecutive=2)
    submit_payloads(client, "dev-a", [
        (1, {"temp": 40, "hum": 50}),
        (2, {"temp": 40, "hum": 10}),
        (3, {"temp": 40, "hum": 10}),
    ])
    by_rule = {}
    for a in alerts(client):
        by_rule.setdefault(a["rule_id"], []).append((a["start_seq"], a["end_seq"]))
    assert by_rule[1] == [(1, 2)]  # temp violated from seq 1
    assert by_rule[2] == [(2, 3)]  # hum violated from seq 2


def test_rule_starts_from_creation_no_history_backfill(client):
    submit(client, "dev-a", [1, 2, 3])  # committed before the rule exists
    create_rule(client, consecutive=2)
    assert alerts(client) == []  # no retroactive evaluation
    submit(client, "dev-a", [4, 5])
    assert spans(alerts(client)) == [(4, 5)]


# --------------------------------------------------------------------- #
# commit-path semantics
# --------------------------------------------------------------------- #

def test_buffered_records_are_not_evaluated_until_committed(client):
    create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 3, 4, 5])  # 1 committed; 3,4,5 buffered
    assert alerts(client) == []
    assert rule_state(client)["streak"] == 1
    submit(client, "dev-a", [2])  # commits 2, drains 3,4,5 in seq order
    assert spans(alerts(client)) == [(1, 3)]  # fired when 3 drained
    assert rule_state(client)["streak"] == 5


def test_drain_evaluates_each_record_not_just_the_batch(client):
    create_rule(client, consecutive=2)
    submit_payloads(client, "dev-a", [
        (1, {"temp": 40}),
        (3, {"temp": 40}),
        (4, {"temp": 10}),  # breaks the run once drained
        (5, {"temp": 40}),
    ])
    submit(client, "dev-a", [2], value=40.0)
    # evaluation order 1..5 -> streaks 1, 2 (alert@2), 3, 0, 1
    assert spans(alerts(client)) == [(1, 2)]
    assert rule_state(client) == {"last_seq": 5, "streak": 1, "alerted": False}


def test_duplicate_batch_does_not_double_count(client):
    create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 2, 3])
    body = submit(client, "dev-a", [1, 2, 3])  # full batch retry
    assert all(r["status"] == "duplicate" for r in body["results"])
    assert len(alerts(client)) == 1
    # the retry neither extends nor resets the streak
    assert rule_state(client) == {"last_seq": 3, "streak": 3, "alerted": True}
    submit(client, "dev-a", [4])  # still the same run: no second alert
    assert len(alerts(client)) == 1


def test_stale_and_out_of_window_records_are_not_evaluated(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [100, 101])  # bootstrap at 100; both violate -> alert
    assert spans(alerts(client)) == [(100, 101)]
    body = submit(client, "dev-a", [40])  # stale: below the window, never recorded
    assert body["results"][0]["status"] == "stale"
    body = submit(client, "dev-a", [10_000])  # out_of_window
    assert body["results"][0]["status"] == "out_of_window"
    # neither record touched the rule: no new alert, streak untouched
    assert spans(alerts(client)) == [(100, 101)]
    assert rule_state(client) == {"last_seq": 101, "streak": 2, "alerted": True}


# --------------------------------------------------------------------- #
# gap (断档) interplay
# --------------------------------------------------------------------- #

def test_declared_gap_breaks_streak_and_backfill_does_not_recompute(client):
    create_rule(client, consecutive=3)
    submit(client, "dev-a", [1, 2])  # streak 2
    submit(client, "dev-a", [5])     # buffered behind the pending gap 3..4
    time.sleep(0.3)                  # let the gap expire
    win = client.get("/api/v1/devices/dev-a/window").json()  # lazy 断档 declaration
    assert [(g["start_seq"], g["end_seq"]) for g in win["gaps"]] == [(3, 4)]
    # the declared gap broke the run; 5 drained to committed starts a fresh one
    assert rule_state(client)["streak"] == 1
    assert alerts(client) == []

    # 断档补录: replay fills 3 and 4 with violating values -> must NOT count
    client.post("/api/v1/devices/dev-a/gaps/1/replay")
    body = submit(client, "dev-a", [3, 4])
    assert [r["status"] for r in body["results"]] == ["gap_filled", "gap_filled"]
    assert rule_state(client)["streak"] == 1

    submit(client, "dev-a", [6, 7])  # streak 2, then 3 -> alert
    assert spans(alerts(client)) == [(5, 7)]


def test_recovery_after_gap_rearms_rule(client):
    create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])  # alert fires at 2
    submit(client, "dev-a", [4])     # buffered behind pending gap 3
    time.sleep(0.3)
    client.get("/api/v1/devices/dev-a/window")  # declares gap [3,3], drains 4
    assert len(alerts(client)) == 1  # the gap broke the alerted run, no new alert
    submit(client, "dev-a", [5])     # streak 2 after re-arm -> fires again
    assert spans(alerts(client)) == [(1, 2), (4, 5)]


# --------------------------------------------------------------------- #
# alert queries
# --------------------------------------------------------------------- #

def test_alerts_listing_and_rule_filter(client):
    rule = create_rule(client, consecutive=2)
    submit(client, "dev-a", [1, 2])
    body = client.get("/api/v1/devices/dev-a/alerts").json()
    assert body["device_id"] == "dev-a"
    assert len(body["alerts"]) == 1
    alert = body["alerts"][0]
    assert alert["alert_id"] == 1
    assert alert["rule_id"] == rule["rule_id"]
    assert alert["created_at"] > 0

    assert alerts(client, rule_id=rule["rule_id"]) == body["alerts"]
    assert alerts(client, rule_id=999) == []
    assert alerts(client, "unknown-device") == []


# --------------------------------------------------------------------- #
# acknowledgement workflow
# --------------------------------------------------------------------- #

# Every historical field that existed before the workflow; acknowledgement
# must never drop or rename any of them.
ALERT_HISTORY_FIELDS = {
    "alert_id", "rule_id", "device_id", "field", "op", "threshold",
    "consecutive", "start_seq", "end_seq", "trigger_value", "created_at",
}


def make_alert(client, device="dev-a"):
    create_rule(client, device=device, consecutive=2)
    submit(client, device, [1, 2])
    return alerts(client, device)[0]


def ack(client, alert_id, by="ops-alice", device="dev-a"):
    resp = client.post(
        f"/api/v1/devices/{device}/alerts/{alert_id}/acknowledge",
        json={"acknowledged_by": by},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_new_alert_is_born_open_with_null_ack_fields(client):
    alert = make_alert(client)
    assert alert["status"] == "open"
    assert alert["acknowledged_by"] is None
    assert alert["acknowledged_at"] is None
    # every pre-workflow field is still present
    assert ALERT_HISTORY_FIELDS <= set(alert)


def test_acknowledge_records_confirmer_and_time(client):
    alert = make_alert(client)
    before = time.time()
    result = ack(client, alert["alert_id"], by="ops-alice")
    after = time.time()

    assert result["status"] == "acknowledged"
    assert result["acknowledged_by"] == "ops-alice"
    assert before <= result["acknowledged_at"] <= after
    assert ALERT_HISTORY_FIELDS <= set(result)
    # historical fields are untouched by the confirmation
    for key in ALERT_HISTORY_FIELDS:
        assert result[key] == alert[key]

    # the list endpoint shows the persisted state, still with all fields
    listed = alerts(client)[0]
    assert listed == result


def test_repeated_acknowledgement_is_idempotent_and_keeps_first_confirmation(client):
    alert = make_alert(client)
    first = ack(client, alert["alert_id"], by="ops-alice")

    # a retry (even from a different confirmer) returns the current record
    # and must not overwrite the first confirmation
    second = ack(client, alert["alert_id"], by="ops-bob")
    third = ack(client, alert["alert_id"], by="ops-alice")
    for repeated in (second, third):
        assert repeated == first
        assert repeated["acknowledged_by"] == "ops-alice"
        assert repeated["acknowledged_at"] == first["acknowledged_at"]

    assert alerts(client)[0] == first


def test_status_filter_lists_open_and_acknowledged(client):
    create_rule(client, consecutive=1)
    # two independent violating runs -> two alerts
    submit(client, "dev-a", [1])
    submit(client, "dev-a", [2], value=10.0)  # recovery re-arms the rule
    submit(client, "dev-a", [3])
    assert [a["alert_id"] for a in alerts(client)] == [1, 2]

    # default (no filter) keeps the whole history
    assert [a["status"] for a in alerts(client)] == ["open", "open"]
    ack(client, 1, by="ops-alice")

    assert [a["alert_id"] for a in alerts(client, status="open")] == [2]
    assert [a["alert_id"] for a in alerts(client, status="acknowledged")] == [1]
    got = alerts(client, status="acknowledged")[0]
    assert got["acknowledged_by"] == "ops-alice"
    assert ALERT_HISTORY_FIELDS <= set(got)

    # status composes with rule_id; invalid status is a validation error
    assert alerts(client, rule_id=1, status="acknowledged")[0]["alert_id"] == 1
    resp = client.get("/api/v1/devices/dev-a/alerts", params={"status": "closed"})
    assert resp.status_code == 422


def test_acknowledge_unknown_or_other_device_alert_404(client):
    alert = make_alert(client, device="dev-a")
    assert client.post(
        "/api/v1/devices/dev-a/alerts/999/acknowledge",
        json={"acknowledged_by": "ops-alice"},
    ).status_code == 404
    # alert 1 belongs to dev-a; the same id under another device is unknown
    assert client.post(
        f"/api/v1/devices/other/alerts/{alert['alert_id']}/acknowledge",
        json={"acknowledged_by": "ops-alice"},
    ).status_code == 404
    # and the failed lookup must not have changed the alert
    assert alerts(client)[0]["status"] == "open"


def test_acknowledge_request_validation(client):
    make_alert(client)
    path = "/api/v1/devices/dev-a/alerts/1/acknowledge"
    assert client.post(path, json={}).status_code == 422
    assert client.post(path, json={"acknowledged_by": ""}).status_code == 422
    assert client.post(path, json={"acknowledged_by": "x" * 129}).status_code == 422
    assert client.post(path, json={"acknowledged_by": 123}).status_code == 422
    assert alerts(client)[0]["status"] == "open"


def test_pre_workflow_database_is_migrated(tmp_path):
    """An existing DB whose alerts table predates the workflow must gain the
    acknowledgement columns on startup (e.g. an old Docker /data volume)."""
    import sqlite3

    from app.db import connect, init_db

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
            "end_seq INTEGER, trigger_value REAL, created_at REAL)"
        )
        raw.execute(
            "INSERT INTO alerts (rule_id, device_id, field, op, threshold, "
            "consecutive, start_seq, end_seq, trigger_value, created_at) "
            "VALUES (1, 'dev-a', 'temp', 'gt', 30, 2, 1, 2, 40, 1.0)"
        )

    init_db(path)  # adds status / acknowledged_by / acknowledged_at
    init_db(path)  # migration is idempotent

    conn = connect(path)
    try:
        row = conn.execute("SELECT * FROM alerts WHERE id = 1").fetchone()
        assert row["status"] == "open"
        assert row["acknowledged_by"] is None
        assert row["acknowledged_at"] is None
    finally:
        conn.close()

    # the migrated row behaves like any open alert through the API
    settings = Settings(db_path=path, sweep_enabled=False)
    with TestClient(create_app(settings)) as c:
        resp = c.post(
            "/api/v1/devices/dev-a/alerts/1/acknowledge",
            json={"acknowledged_by": "ops-alice"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["status"] == "acknowledged"


# --------------------------------------------------------------------- #
# concurrent acknowledgement: real server, parallel confirmations
# --------------------------------------------------------------------- #

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_concurrent_acknowledgements_have_one_consistent_winner(tmp_path):
    import socket
    import threading
    from concurrent.futures import ThreadPoolExecutor

    import httpx
    import uvicorn

    settings = Settings(
        db_path=str(tmp_path / "conc_ack.db"),
        window_size=50,
        gap_timeout_seconds=600,
        sweep_enabled=False,
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
            r = c.post(
                "/api/v1/devices/dev-a/alert-rules",
                json={"field": "temp", "op": "gt", "threshold": 30.0,
                      "consecutive": 2},
            )
            assert r.status_code == 201, r.text
            r = c.post(
                "/api/v1/batches",
                json={"device_id": "dev-a",
                      "records": [{"seq": s, "payload": {"temp": 40}} for s in (1, 2)]},
            )
            assert r.status_code == 200, r.text

        def confirm(worker: int):
            with httpx.Client(base_url=base, timeout=10) as c:
                r = c.post(
                    "/api/v1/devices/dev-a/alerts/1/acknowledge",
                    json={"acknowledged_by": f"ops-{worker}"},
                )
                assert r.status_code == 200, r.text
                return r.json()

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(confirm, range(16)))

        # every racer saw the same record; exactly one confirmer won and its
        # identity/time are identical across all responses and the list view
        winners = {(r["acknowledged_by"], r["acknowledged_at"]) for r in results}
        assert len(winners) == 1
        assert all(r["status"] == "acknowledged" for r in results)
        assert len({r["alert_id"] for r in results}) == 1

        with httpx.Client(base_url=base, timeout=10) as c:
            stored = c.get("/api/v1/devices/dev-a/alerts").json()["alerts"][0]
            assert (stored["acknowledged_by"], stored["acknowledged_at"]) == next(iter(winners))
            assert (
                c.get("/api/v1/devices/dev-a/alerts",
                      params={"status": "open"}).json()["alerts"] == []
            )
            assert len(c.get(
                "/api/v1/devices/dev-a/alerts",
                params={"status": "acknowledged"}).json()["alerts"]) == 1
    finally:
        server.should_exit = True
        thread.join(timeout=5)
