"""Consecutive-threshold alerting on committed telemetry, with rule versioning.

Rules are device-scoped series (``alert_rules``); each series owns an ordered
set of immutable parameter versions (``alert_rule_versions``). Version N+1
takes effect at a configurable sequence ``effective_seq``: when a record with
that seq is first evaluated, evaluation silently switches to the new version,
which starts its own streak from zero. Superseded versions — and every alert
row already produced — are never rewritten.

Version selection is atomic and per record:

    SELECT ... FROM alert_rule_versions
    WHERE rule_id = ? AND effective_seq <= ?
    ORDER BY effective_seq DESC LIMIT 1

``UNIQUE(rule_id, effective_seq)`` guarantees the pick is unique. Every
commit — a direct commit and each record drained buffered -> committed —
runs inside the window engine's BEGIN IMMEDIATE transaction, so a rule
modified concurrently with a batch cannot be seen by half the records of a
seq run: the modification either committed before a given seq is evaluated
(its version wins) or after (the previous version wins). Two concurrent
modifications also serialize; the second gets the next version_no and must
choose a strictly larger effective_seq. A new effective_seq may not precede
the device's commit frontier (next_expected): a version back-dated into
already-committed history could never evaluate the records it claims to own.
Targeting still-buffered seqs is allowed.

The engine is driven exclusively by the window engine's commit path: a
version advances only when a record turns `committed` for the first time, in
ascending seq order. Duplicates, stale records and gap backfills
(gap_filled) never touch state, so retried batches cannot double-count and
断档补录 does not recompute history.

A declared gap (断档), a missing field or a non-numeric value breaks the
consecutive run of every version that could still evaluate records (the one
effective at the jump seq and all later ones); fully-superseded versions stay
frozen. When a run first reaches N consecutive violations exactly one alert
row is written and pinned to its version; the same run never fires again.

Anomaly-segment end records (``alert_ends``)
--------------------------------------------

A fired segment is not over when its alert is written: it stays *active*
until the violating run actually ends. The end is recorded separately from
the immutable alert row (the parameter snapshot and acknowledgement state
never change), exactly once per alert, with an end seq, server time and one
of three reasons:

* ``record``  — the next record turning committed is in-range (no violation),
  or its watched field is missing/non-numeric. ``end_seq`` is that record.
* ``gap``     — a gap is declared while the segment is active. ``end_seq`` is
  the first seq after the hole (where evaluation resumes).
* ``version`` — a newer version of the rule evaluates its first committed
  record; the superseded version's active segment ends at that seq.

Ending advances only through the commit path: duplicates, records still
``buffered`` and ``gap_filled`` backfills can never end a segment early. The
PRIMARY KEY on ``alert_ends.alert_id`` makes the "at most once" guarantee
atomic; every close is a guarded INSERT inside the window engine's write
transaction. Segments whose run simply never ends stay active forever (their
alert has no end record).

All methods run inside the caller's write transaction (db.write_txn).
"""
from __future__ import annotations

import json
import math
import sqlite3
import time
from typing import Optional

# Comparison operators supported by alert rules.
GT = "gt"
LT = "lt"
OPERATORS = (GT, LT)

# Anomaly-segment end reasons stored in alert_ends.end_reason.
END_RECORD = "record"   # committed in-range / missing-field / non-numeric record
END_GAP = "gap"         # a gap (断档) was declared
END_VERSION = "version"  # a newer rule version took over


def _as_number(value) -> Optional[float]:
    """Return `value` as a finite float, or None if it is not a real number.

    Booleans are excluded (they are ints in Python but not measurements) and
    NaN/Infinity can never satisfy a threshold comparison.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


class RuleNotFound(Exception):
    """No rule series exists for the given (device, rule_id) pair."""


class EffectiveSeqConflict(Exception):
    """Another version of the rule already claims that effective_seq."""


class AlertEngine:
    def __init__(self, conn):
        self.conn = conn

    # ------------------------------------------------------------------ #
    # rule series & version management
    # ------------------------------------------------------------------ #
    def create_rule(self, device_id: str, field: str, op: str,
                    threshold: float, consecutive: int) -> dict:
        """Register a rule series with version 1 effective from seq 0.

        It evaluates records committed from now on (already-committed history
        is not backfilled); effective_seq 0 merely fixes where version 1's
        ownership of the seq axis begins.
        """
        now = time.time()
        cur = self.conn.execute(
            "INSERT INTO alert_rules (device_id, created_at) VALUES (?, ?)",
            (device_id, now),
        )
        rule_id = cur.lastrowid
        self._insert_version(
            rule_id, 1, field, op, threshold, consecutive, 0, now, init_state=True
        )
        return self.get_rule(rule_id)

    def modify_rule(
        self,
        device_id: str,
        rule_id: int,
        field: str,
        op: str,
        threshold: float,
        consecutive: int,
        effective_seq: Optional[int] = None,
    ) -> dict:
        """Append a new immutable version to an existing rule series.

        `effective_seq` is the first seq the new version evaluates; it must be
        strictly greater than the previous version's and not before the
        device's commit frontier (next_expected) — a version backdated into
        already-committed history could never evaluate the records it claims
        to own, so per-seq selection would disagree with what actually fired.
        Targeting buffered (not-yet-committed) seqs is allowed. When omitted
        it defaults to next_expected, i.e. the switch happens at the frontier.
        Returns RuleNotFound for an unknown/foreign rule id and
        EffectiveSeqConflict on a collision/back-dating.
        """
        rule = self.conn.execute(
            "SELECT * FROM alert_rules WHERE id = ? AND device_id = ?",
            (rule_id, device_id),
        ).fetchone()
        if rule is None:
            raise RuleNotFound(rule_id)

        latest = self._latest_version(rule_id)
        dev = self.conn.execute(
            "SELECT next_expected FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        # Rules may be provisioned before the device exists (it bootstraps at
        # its first batch); until then the frontier is unknown and treated as 0.
        frontier = dev["next_expected"] if dev is not None else 0
        if effective_seq is None:
            effective_seq = frontier
        if effective_seq < frontier or effective_seq <= latest["effective_seq"]:
            # Same-seq handover is also a UNIQUE collision; reject back-dated,
            # duplicate and non-monotonic requests with one error.
            raise EffectiveSeqConflict(effective_seq)

        now = time.time()
        version_no = latest["version_no"] + 1
        try:
            self._insert_version(
                rule_id, version_no, field, op, threshold, consecutive,
                effective_seq, now, init_state=True,
            )
        except Exception as exc:  # sqlite3.IntegrityError on UNIQUE(effective_seq)
            if _is_unique_violation(exc):
                raise EffectiveSeqConflict(effective_seq) from exc
            raise
        return self.get_rule(rule_id)

    def _insert_version(self, rule_id: int, version_no: int, field: str, op: str,
                        threshold: float, consecutive: int, effective_seq: int,
                        now: float, init_state: bool = False) -> int:
        cur = self.conn.execute(
            "INSERT INTO alert_rule_versions (rule_id, version_no, field, op, "
            "threshold, consecutive, effective_seq, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rule_id, version_no, field, op, threshold, consecutive,
             effective_seq, now),
        )
        if init_state:
            # A new version starts disarmed: streak 0, never alerted.
            self.conn.execute(
                "INSERT INTO alert_rule_state (version_id, last_seq, streak, alerted, "
                "updated_at) VALUES (?, NULL, 0, 0, ?)",
                (cur.lastrowid, now),
            )
        return cur.lastrowid

    def get_rule(self, rule_id: int) -> Optional[dict]:
        rule = self.conn.execute(
            "SELECT * FROM alert_rules WHERE id = ?", (rule_id,)
        ).fetchone()
        return self._rule_dict(rule) if rule is not None else None

    def list_rules(self, device_id: str) -> list[dict]:
        rules = self.conn.execute(
            "SELECT * FROM alert_rules WHERE device_id = ? ORDER BY id", (device_id,)
        ).fetchall()
        return [self._rule_dict(r) for r in rules]

    def list_versions(self, device_id: str, rule_id: int) -> Optional[list[dict]]:
        """All versions of one rule, oldest first. None for an unknown/foreign rule."""
        rule = self.conn.execute(
            "SELECT 1 FROM alert_rules WHERE id = ? AND device_id = ?",
            (rule_id, device_id),
        ).fetchone()
        if rule is None:
            return None
        versions = self.conn.execute(
            "SELECT * FROM alert_rule_versions WHERE rule_id = ? ORDER BY version_no",
            (rule_id,),
        ).fetchall()
        return [self._version_dict(v) for v in versions]

    def _latest_version(self, rule_id: int):
        return self.conn.execute(
            "SELECT * FROM alert_rule_versions WHERE rule_id = ? "
            "ORDER BY version_no DESC LIMIT 1",
            (rule_id,),
        ).fetchone()

    def _rule_dict(self, rule) -> dict:
        # The "current" rule is the newest version; the GET /alert-rules list
        # stays the shape it had pre-versioning, extended with version fields.
        latest = self._latest_version(rule["id"])
        body = {
            "rule_id": rule["id"],
            "device_id": rule["device_id"],
            "created_at": rule["created_at"],
            "version_count": self.conn.execute(
                "SELECT COUNT(*) AS c FROM alert_rule_versions WHERE rule_id = ?",
                (rule["id"],),
            ).fetchone()["c"],
        }
        body.update(self._version_dict(latest))
        return body

    def _version_dict(self, version) -> dict:
        state = self.conn.execute(
            "SELECT last_seq, streak, alerted FROM alert_rule_state WHERE version_id = ?",
            (version["id"],),
        ).fetchone()
        return {
            "version_no": version["version_no"],
            "rule_version_id": version["id"],
            "field": version["field"],
            "op": version["op"],
            "threshold": version["threshold"],
            "consecutive": version["consecutive"],
            "effective_seq": version["effective_seq"],
            "created_at": version["created_at"],
            "state": {
                "last_seq": state["last_seq"] if state is not None else None,
                "streak": state["streak"] if state is not None else 0,
                "alerted": bool(state["alerted"]) if state is not None else False,
            },
        }

    # ------------------------------------------------------------------ #
    # alert queries
    # ------------------------------------------------------------------ #
    def list_alerts(
        self,
        device_id: str,
        rule_id: Optional[int] = None,
        status: Optional[str] = None,
        version_no: Optional[int] = None,
        version_id: Optional[int] = None,
        active: Optional[bool] = None,
    ) -> list[dict]:
        clauses = ["a.device_id = ?"]
        params: list = [device_id]
        if rule_id is not None:
            clauses.append("a.rule_id = ?")
            params.append(rule_id)
        if status is not None:
            clauses.append("a.status = ?")
            params.append(status)
        if version_no is not None:
            clauses.append("a.version_no = ?")
            params.append(version_no)
        if version_id is not None:
            clauses.append("a.version_id = ?")
            params.append(version_id)
        if active is not None:
            # An alert is active exactly while it has no end record. The
            # acknowledgement status is orthogonal and stays filterable.
            if active:
                clauses.append(
                    "NOT EXISTS (SELECT 1 FROM alert_ends e WHERE e.alert_id = a.id)"
                )
            else:
                clauses.append(
                    "EXISTS (SELECT 1 FROM alert_ends e WHERE e.alert_id = a.id)"
                )
        rows = self.conn.execute(
            f"SELECT a.* FROM alerts a WHERE {' AND '.join(clauses)} ORDER BY a.id", params
        ).fetchall()
        return [self._alert_dict(r) for r in rows]

    def get_alert(self, device_id: str, alert_id: int) -> Optional[dict]:
        """Fetch one device-scoped alert, or None when it does not exist."""
        row = self.conn.execute(
            "SELECT * FROM alerts WHERE device_id = ? AND id = ?",
            (device_id, alert_id),
        ).fetchone()
        return self._alert_dict(row) if row is not None else None

    def acknowledge_alert(
        self, device_id: str, alert_id: int, acknowledged_by: str, now: float
    ) -> Optional[dict]:
        """Idempotently confirm an alert.

        Runs inside the caller's BEGIN IMMEDIATE transaction: the conditional
        UPDATE claims the alert only while it is still 'open', so concurrent
        confirmations serialize on the row and exactly one wins. A repeated
        call matches no row and simply returns the stored record — the first
        confirmer's identity and timestamp can never be rewritten.
        """
        self.conn.execute(
            "UPDATE alerts SET status = 'acknowledged', acknowledged_by = ?, "
            "acknowledged_at = ? WHERE device_id = ? AND id = ? AND status = 'open'",
            (acknowledged_by, now, device_id, alert_id),
        )
        # When cur.rowcount == 0 the alert either does not exist (handled by
        # the following lookup -> None) or was already acknowledged (the
        # existing row is returned untouched).
        return self.get_alert(device_id, alert_id)

    def _alert_dict(self, row) -> dict:
        end = self.conn.execute(
            "SELECT end_seq, end_reason, ended_at FROM alert_ends WHERE alert_id = ?",
            (row["id"],),
        ).fetchone()
        return {
            "alert_id": row["id"],
            "rule_id": row["rule_id"],
            "rule_version_id": row["version_id"],
            "version_no": row["version_no"],
            "device_id": row["device_id"],
            "field": row["field"],
            "op": row["op"],
            "threshold": row["threshold"],
            "consecutive": row["consecutive"],
            "start_seq": row["start_seq"],
            "end_seq": row["end_seq"],
            "trigger_value": row["trigger_value"],
            "created_at": row["created_at"],
            "status": row["status"],
            "acknowledged_by": row["acknowledged_by"],
            "acknowledged_at": row["acknowledged_at"],
            # Separate segment-end record; null while the segment is active.
            "ending": (
                {
                    "end_seq": end["end_seq"],
                    "end_reason": end["end_reason"],
                    "ended_at": end["ended_at"],
                }
                if end is not None
                else None
            ),
        }

    # ------------------------------------------------------------------ #
    # window-engine hooks (must run inside the caller's write transaction)
    # ------------------------------------------------------------------ #
    def on_committed(self, device_id: str, seq: int, payload_json: str, now: float) -> None:
        """Advance the per-seq-effective version of every rule by one record."""
        rules = self.conn.execute(
            "SELECT id FROM alert_rules WHERE device_id = ? ORDER BY id", (device_id,)
        ).fetchall()
        if not rules:
            return
        try:
            payload = json.loads(payload_json)
        except ValueError:
            payload = None
        if not isinstance(payload, dict):
            payload = None
        for rule in rules:
            version = self._version_for_seq(rule["id"], seq)
            if version is None:
                # No version owns this seq (versions all start above it): the
                # record is invisible to the rule.
                continue
            # A newer version evaluates this seq: the first committed record a
            # new version takes over ends every still-active segment of the
            # superseded versions, reason 'version'. This only fires for the
            # takeover record itself — afterwards the old versions have no open
            # alert left to close.
            self._close_superseded_alerts(device_id, rule["id"], version["id"],
                                          seq, now)
            raw = payload.get(version["field"]) if payload is not None else None
            self._advance_version(device_id, version, seq, _as_number(raw), now)

    def _version_for_seq(self, rule_id: int, seq: int):
        """The unique version of rule_id whose effective_seq range covers seq.

        Atomic within the caller's write txn; the UNIQUE(effective_seq)
        constraint makes this pick unambiguous even when a modification and a
        batch commit race — they serialize, so the winner is deterministic.
        """
        return self.conn.execute(
            "SELECT * FROM alert_rule_versions WHERE rule_id = ? AND effective_seq <= ? "
            "ORDER BY effective_seq DESC LIMIT 1",
            (rule_id, seq),
        ).fetchone()

    def on_gap_declared(self, device_id: str, at_seq: int, now: float) -> None:
        """A declared 断档 breaks runs of every live version.

        Versions already fully superseded before `at_seq` are frozen (their
        streak only ever described a completed history). The version effective
        at `at_seq` and every later version reset, including versions scheduled
        to take effect further ahead that had not evaluated anything yet.

        Every active segment of those reset versions ends here with reason
        'gap'; ``end_seq`` is `at_seq`, the first seq after the declared hole
        where evaluation resumes. The end record and the streak reset happen in
        the same write transaction.
        """
        # Close active segments first: the same live-version set the UPDATE
        # below resets — the version effective at at_seq and every later one.
        # The alerted=1 predicate matches only versions mid-segment (an
        # un-alerted version has no open alert) and naturally skips versions
        # whose segment was already ended (reset) or superseded earlier.
        self.conn.execute(
            "INSERT INTO alert_ends (alert_id, end_seq, end_reason, ended_at) "
            "SELECT a.id, ?, ?, ? FROM alerts a "
            "JOIN alert_rule_versions v ON v.id = a.version_id "
            "JOIN alert_rules r ON r.id = v.rule_id "
            "JOIN alert_rule_state s ON s.version_id = v.id "
            "WHERE r.device_id = ? AND s.alerted = 1 "
            "AND v.effective_seq >= ("
            "  SELECT COALESCE(MAX(effective_seq), 0) FROM alert_rule_versions "
            "  WHERE rule_id = v.rule_id AND effective_seq <= ?) "
            "AND NOT EXISTS (SELECT 1 FROM alert_ends e WHERE e.alert_id = a.id)",
            (at_seq, END_GAP, now, device_id, at_seq),
        )
        self.conn.execute(
            "UPDATE alert_rule_state SET streak = 0, alerted = 0, updated_at = ? "
            "WHERE version_id IN ("
            "  SELECT v.id FROM alert_rule_versions v JOIN alert_rules r ON r.id = v.rule_id "
            "  WHERE r.device_id = ? AND v.effective_seq >= ("
            "    SELECT COALESCE(MAX(effective_seq), 0) FROM alert_rule_versions "
            "    WHERE rule_id = v.rule_id AND effective_seq <= ?))",
            (now, device_id, at_seq),
        )

    def _advance_version(self, device_id: str, version, seq: int,
                         value: Optional[float], now: float) -> None:
        state = self.conn.execute(
            "SELECT streak, alerted FROM alert_rule_state WHERE version_id = ?",
            (version["id"],),
        ).fetchone()
        streak = state["streak"] if state is not None else 0
        alerted = bool(state["alerted"]) if state is not None else False

        violates = value is not None and (
            value > version["threshold"] if version["op"] == GT else value < version["threshold"]
        )
        if violates:
            streak += 1
            if streak >= version["consecutive"] and not alerted:
                self._fire(device_id, version, seq, streak, value, now)
                alerted = True
        else:
            # Normal value, missing field or non-numeric value: the run
            # breaks and the version re-arms for the next one. This first
            # closing record also ends the fired segment once, reason 'record'.
            if alerted:
                self._close_latest_open_alert(version["id"], seq, END_RECORD, now)
            streak, alerted = 0, False

        self.conn.execute(
            "UPDATE alert_rule_state SET last_seq = ?, streak = ?, alerted = ?, "
            "updated_at = ? WHERE version_id = ?",
            (seq, streak, int(alerted), now, version["id"]),
        )

    def _close_latest_open_alert(self, version_id: int, end_seq: int,
                                 reason: str, now: float) -> None:
        """End this version's newest not-yet-ended alert, once.

        Guarded with NOT EXISTS so a re-entrant/duplicated call can never raise
        on the PRIMARY KEY; a segment is ended at most once.
        """
        self.conn.execute(
            "INSERT INTO alert_ends (alert_id, end_seq, end_reason, ended_at) "
            "SELECT a.id, ?, ?, ? FROM alerts a "
            "WHERE a.version_id = ? "
            "AND NOT EXISTS (SELECT 1 FROM alert_ends e WHERE e.alert_id = a.id) "
            "ORDER BY a.id DESC LIMIT 1",
            (end_seq, reason, now, version_id),
        )

    def _close_superseded_alerts(self, device_id: str, rule_id: int,
                                 new_version_id: int, seq: int, now: float) -> None:
        """End active segments of versions the current one just superseded.

        Called for every committed record with the version effective at that
        seq: when it differs from the versions that produced earlier active
        segments, those segments end at `seq` with reason 'version'. Versions
        at or after `new_version_id` (including it) are untouched, as are
        alerts already ended. The ``effective_seq <= ?`` bound limits the set
        to versions that actually owned records up to this takeover point.
        """
        self.conn.execute(
            "INSERT INTO alert_ends (alert_id, end_seq, end_reason, ended_at) "
            "SELECT a.id, ?, ?, ? FROM alerts a "
            "JOIN alert_rule_versions v ON v.id = a.version_id "
            "WHERE a.device_id = ? AND a.rule_id = ? AND v.id < ? "
            "AND v.effective_seq <= ? "
            "AND NOT EXISTS (SELECT 1 FROM alert_ends e WHERE e.alert_id = a.id)",
            (seq, END_VERSION, now, device_id, rule_id, new_version_id, seq),
        )

    def _fire(self, device_id: str, version, seq: int, streak: int,
              value: float, now: float) -> None:
        self.conn.execute(
            "INSERT INTO alerts (rule_id, version_id, version_no, device_id, field, op, "
            "threshold, consecutive, start_seq, end_seq, trigger_value, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                version["rule_id"], version["id"], version["version_no"],
                device_id,
                version["field"], version["op"], version["threshold"],
                version["consecutive"],
                seq - streak + 1,  # first seq of the violating run
                seq,               # record that reached the consecutive count
                value, now,
            ),
        )


def _is_unique_violation(exc: Exception) -> bool:
    return isinstance(exc, sqlite3.IntegrityError) and "UNIQUE" in str(exc)
