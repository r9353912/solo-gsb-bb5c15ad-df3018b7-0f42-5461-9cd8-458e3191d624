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

Once a run has fired, its alert is *active* until the violating segment ends.
The terminator is recorded separately in `alert_closings` (the alert row and
its acknowledgement state are never rewritten):

* 'record'  — the first committed record that no longer violates (a normal
  value, a missing field or a non-numeric value), at that record's seq;
* 'gap'     — a 断档 declared before the run could continue; end_seq is the
  last seq of the declared hole (the new run can only resume past it);
* 'version' — the first committed record taken over by a newer rule version
  ends the old version's segment, at the takeover record's seq.

Each alert is closed at most once (PRIMARY KEY on alert_closings.alert_id +
INSERT OR IGNORE): the first terminator wins, even when several apply within
the same drain. Closure is driven exclusively by first-time commits and by
gap declaration, so duplicates, still-buffered records and gap_filled
backfills can never close a segment early.

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

# Segment-closing reasons persisted in alert_closings.
CLOSE_RECORD = "record"    # first committed non-violating record ended the segment
CLOSE_GAP = "gap"          # a declared 断档 ended the segment
CLOSE_VERSION = "version"  # a newer rule version took over the first later record


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
            # Active = fired segment not yet ended: no closing row exists.
            clauses.append(
                "c.alert_id IS NOT NULL" if not active else "c.alert_id IS NULL"
            )
        rows = self.conn.execute(
            f"SELECT a.*, c.end_seq AS close_end_seq, c.reason AS close_reason, "
            f"c.ended_at AS close_ended_at FROM alerts a "
            f"LEFT JOIN alert_closings c ON c.alert_id = a.id "
            f"WHERE {' AND '.join(clauses)} ORDER BY a.id",
            params,
        ).fetchall()
        return [self._alert_dict(r) for r in rows]

    def get_alert(self, device_id: str, alert_id: int) -> Optional[dict]:
        """Fetch one device-scoped alert, or None when it does not exist."""
        row = self.conn.execute(
            "SELECT a.*, c.end_seq AS close_end_seq, c.reason AS close_reason, "
            "c.ended_at AS close_ended_at FROM alerts a "
            "LEFT JOIN alert_closings c ON c.alert_id = a.id "
            "WHERE a.device_id = ? AND a.id = ?",
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

    @staticmethod
    def _alert_dict(row) -> dict:
        # The closing record is stored separately; an alert without one is an
        # active (segment still open) alert. The acknowledgement state on the
        # alert row is independent and untouched by a closing.
        closing = None
        if row["close_reason"] is not None:
            closing = {
                "end_seq": row["close_end_seq"],
                "reason": row["close_reason"],
                "ended_at": row["close_ended_at"],
            }
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
            "active": closing is None,
            "closing": closing,
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
            # Version handover: this is the first committed record the new
            # version owns, so the previous version's still-open segment ends
            # here with reason 'version'. Done before advancing so a record
            # that itself ends the segment can only ever record one closing.
            self._close_older_versions(rule["id"], version["id"], seq, CLOSE_VERSION, now)
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

    def on_gap_declared(self, device_id: str, at_seq: int, gap_start_seq: int,
                        gap_end_seq: int, now: float) -> None:
        """A declared 断档 breaks runs and ends the segments it cut off.

        Streak reset targets the versions that can still evaluate after the
        jump: the one effective at `at_seq` (resume seq) and every later one.
        Versions already fully superseded before `at_seq` stay frozen, as
        before. Closing is broader: the hole spans [gap_start_seq, gap_end_seq],
        so any still-open alert fired by the version effective at the gap's
        start or a later version is terminated by the gap itself — including a
        version superseded only by a newer one scheduled *inside* the hole
        (which never evaluates before the jump); without this, that old
        segment could stay active forever waiting for a takeover record that
        belongs to a different lifecycle. `gap_end_seq` is recorded as the
        segment's end seq: the run can only resume past the hole.
        """
        reset_version_ids = [
            r["id"]
            for r in self.conn.execute(
                "SELECT v.id FROM alert_rule_versions v JOIN alert_rules r ON r.id = v.rule_id "
                "WHERE r.device_id = ? AND v.effective_seq >= ("
                "  SELECT COALESCE(MAX(effective_seq), 0) FROM alert_rule_versions "
                "  WHERE rule_id = v.rule_id AND effective_seq <= ?)",
                (device_id, at_seq),
            ).fetchall()
        ]
        close_version_ids = [
            r["id"]
            for r in self.conn.execute(
                "SELECT v.id FROM alert_rule_versions v JOIN alert_rules r ON r.id = v.rule_id "
                "WHERE r.device_id = ? AND v.effective_seq >= ("
                "  SELECT COALESCE(MAX(effective_seq), 0) FROM alert_rule_versions "
                "  WHERE rule_id = v.rule_id AND effective_seq <= ?)",
                (device_id, gap_start_seq),
            ).fetchall()
        ]
        # Close first (broader set), then reset the still-live streaks. Either
        # INSERT OR IGNORE is a no-op for alerts already closed, so the overlap
        # of the two sets is harmless and every alert closes at most once.
        if close_version_ids:
            self._close_versions(close_version_ids, gap_end_seq, CLOSE_GAP, now)
        if reset_version_ids:
            placeholders = ",".join("?" for _ in reset_version_ids)
            self.conn.execute(
                f"UPDATE alert_rule_state SET streak = 0, alerted = 0, updated_at = ? "
                f"WHERE version_id IN ({placeholders})",
                (now, *reset_version_ids),
            )

    def _close_older_versions(self, rule_id: int, current_version_id: int,
                              end_seq: int, reason: str, now: float) -> None:
        """Close still-open alerts of every older version of one rule.

        Called when `current_version_id` evaluates its first committed record:
        the previous version can never evaluate again, so any segment it still
        had open ends at this takeover record.
        """
        self.conn.execute(
            "INSERT OR IGNORE INTO alert_closings (alert_id, end_seq, reason, ended_at) "
            "SELECT a.id, ?, ?, ? FROM alerts a WHERE a.rule_id = ? "
            "AND a.version_id < ? AND NOT EXISTS ("
            "  SELECT 1 FROM alert_closings c WHERE c.alert_id = a.id)",
            (end_seq, reason, now, rule_id, current_version_id),
        )

    def _close_versions(self, version_ids: list[int], end_seq: int,
                        reason: str, now: float) -> None:
        """Close every still-open alert fired by any of the given versions."""
        if not version_ids:
            return
        placeholders = ",".join("?" for _ in version_ids)
        self.conn.execute(
            f"INSERT OR IGNORE INTO alert_closings (alert_id, end_seq, reason, ended_at) "
            f"SELECT a.id, ?, ?, ? FROM alerts a WHERE a.version_id IN ({placeholders}) "
            f"AND NOT EXISTS ("
            f"  SELECT 1 FROM alert_closings c WHERE c.alert_id = a.id)",
            (end_seq, reason, now, *version_ids),
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
            # breaks and the version re-arms for the next one. If the run
            # that just ended had already fired, this first non-violating
            # record closes its alert with reason 'record'.
            if alerted:
                self._close_versions([version["id"]], seq, CLOSE_RECORD, now)
            streak, alerted = 0, False

        self.conn.execute(
            "UPDATE alert_rule_state SET last_seq = ?, streak = ?, alerted = ?, "
            "updated_at = ? WHERE version_id = ?",
            (seq, streak, int(alerted), now, version["id"]),
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
