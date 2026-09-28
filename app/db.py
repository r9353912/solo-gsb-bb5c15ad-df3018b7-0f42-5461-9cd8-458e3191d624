"""SQLite access layer.

One connection per request (created by the FastAPI dependency in main.py).
WAL mode + busy_timeout allow concurrent readers and serialized writers;
all mutations happen inside BEGIN IMMEDIATE transactions (see write_txn),
which keeps per-device window state consistent under concurrent requests.

Schema evolution
----------------
Alert rules are versioned (see alerts.py): `alert_rules` is the immutable
rule series and `alert_rule_versions` holds every parameter version with the
seq at which it takes effect. Databases created before versioning (the old
wide `alert_rules` shape, with or without the acknowledgement columns on
`alerts`) are upgraded in place by init_db: each old rule becomes version 1
(effective from seq 0), its state row is carried over and every historical
alert is pinned to that version.

Fired alerts are closed at most once; the end-of-run record lives in its own
`alert_closings` table. Because it is created with CREATE TABLE IF NOT
EXISTS in SCHEMA, an old database gains it automatically and idempotently on
startup — pre-existing alerts simply start without a closing row (active).
"""
from __future__ import annotations

import os
import sqlite3
import time
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id        TEXT PRIMARY KEY,
    next_expected    INTEGER NOT NULL,          -- next contiguous sequence expected
    gap_since        REAL,                      -- when the current pending gap was first seen
    duplicates_count INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    device_id   TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    payload     TEXT NOT NULL,                  -- JSON payload as received
    status      TEXT NOT NULL CHECK (status IN ('committed', 'buffered', 'gap_filled')),
    received_at REAL NOT NULL,
    PRIMARY KEY (device_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_records_device_status ON records(device_id, status);

CREATE TABLE IF NOT EXISTS gaps (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    start_seq    INTEGER NOT NULL,
    end_seq      INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('open', 'replaying', 'recovered')),
    replay_count INTEGER NOT NULL DEFAULT 0,
    detected_at  REAL NOT NULL,                 -- when the gap was first observed
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gaps_device ON gaps(device_id, status);

-- A rule is an immutable *series* per device; all mutable parameters live in
-- alert_rule_versions. Modifying a rule appends a new version instead of
-- overwriting, so historical versions and the alerts they produced are frozen.
CREATE TABLE IF NOT EXISTS alert_rules (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id  TEXT NOT NULL,
    created_at REAL NOT NULL                    -- series birth (version 1 creation) time
);
CREATE INDEX IF NOT EXISTS idx_alert_rules_device ON alert_rules(device_id);

CREATE TABLE IF NOT EXISTS alert_rule_versions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id       INTEGER NOT NULL REFERENCES alert_rules (id) ON DELETE CASCADE,
    version_no    INTEGER NOT NULL,             -- 1-based, increasing per rule
    field         TEXT NOT NULL,                -- top-level payload key to inspect
    op            TEXT NOT NULL CHECK (op IN ('gt', 'lt')),
    threshold     REAL NOT NULL,
    consecutive  INTEGER NOT NULL CHECK (consecutive >= 1),
    -- First record seq this version evaluates. For any given seq the engine
    -- picks the unique version with the greatest effective_seq <= seq; the
    -- UNIQUE constraint below makes that choice unambiguous (two versions of
    -- one rule can never take effect at the same seq).
    effective_seq INTEGER NOT NULL,
    created_at    REAL NOT NULL,
    UNIQUE (rule_id, version_no),
    UNIQUE (rule_id, effective_seq)
);
-- The two UNIQUE constraints above already maintain the indexes used for
-- monotonic version lookup (effective_seq <= seq ORDER BY effective_seq DESC).

-- Evaluation state is keyed per *version*: when a new version takes over at
-- its effective_seq it starts with a fresh streak, while the superseded
-- version's last_seq/streak/alerted stays frozen for the version history.
CREATE TABLE IF NOT EXISTS alert_rule_state (
    version_id INTEGER PRIMARY KEY REFERENCES alert_rule_versions (id) ON DELETE CASCADE,
    last_seq   INTEGER,                     -- last committed seq evaluated (NULL = none yet)
    streak     INTEGER NOT NULL DEFAULT 0,  -- current run of consecutive violations
    alerted    INTEGER NOT NULL DEFAULT 0,  -- 1 while the current run has fired its alert
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id       INTEGER NOT NULL REFERENCES alert_rules (id) ON DELETE CASCADE,
    -- Version that produced this alert (its end_seq was evaluated under it).
    -- Alert rows are pinned forever: editing the rule later only adds a new
    -- version and never rewrites that link or the snapshot columns below.
    version_id    INTEGER NOT NULL REFERENCES alert_rule_versions (id),
    version_no    INTEGER NOT NULL,
    device_id     TEXT NOT NULL,
    field         TEXT NOT NULL,            -- rule snapshot, so alerts survive rule changes
    op            TEXT NOT NULL,
    threshold     REAL NOT NULL,
    consecutive  INTEGER NOT NULL,
    start_seq     INTEGER NOT NULL,         -- first seq of the violating run
    end_seq       INTEGER NOT NULL,         -- seq at which the run reached `consecutive`
    trigger_value REAL NOT NULL,
    created_at    REAL NOT NULL,
    -- Acknowledgement workflow: every alert is born 'open' and can be
    -- acknowledged exactly once; the first confirmation's identity/timestamp
    -- are frozen for the lifetime of the row.
    status          TEXT NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'acknowledged')),
    acknowledged_by TEXT,                   -- identifier of the first confirmer
    acknowledged_at REAL                    -- server time of the first confirmation
);
CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts(device_id, id);
-- idx_alerts_version is created after startup migrations: an old alerts table
-- has no version_id column until _migrate_rule_versioning adds it.

-- End-of-run records live in their own table, separate from the immutable
-- alert snapshot: an alert is fired once and closed at most once. The closing
-- describes what terminated the violating segment:
--   'record'  - the first committed record after the fired run that does not
--               violate (normal value / missing field / non-numeric value);
--   'gap'     - a 断档 was declared before the run could continue;
--   'version' - a newer rule version took over the first committed record
--               after the run, freezing the old version's segment.
-- end_seq/ended_at identify that terminator. The PRIMARY KEY on alert_id
-- guarantees one closing per alert; INSERT OR IGNORE makes that idempotent
-- under concurrent serialized transactions. The alerts row (snapshot +
-- acknowledgement state) is never rewritten by a closing.
CREATE TABLE IF NOT EXISTS alert_closings (
    alert_id  INTEGER PRIMARY KEY REFERENCES alerts (id) ON DELETE CASCADE,
    end_seq   INTEGER NOT NULL,             -- seq that ended the violating segment
    reason    TEXT NOT NULL CHECK (reason IN ('record', 'gap', 'version')),
    ended_at  REAL NOT NULL
);
-- The PRIMARY KEY index on alert_id also serves the LEFT JOIN / NOT EXISTS
-- used by the active filter ("alerts without a closing row").
"""

# Idempotent column migrations for databases created before the alerts table
# gained the acknowledgement columns (e.g. an existing Docker /data volume).
# Safe to re-run: columns already present are skipped.
ALERT_COLUMN_MIGRATIONS = (
    (
        "status",
        "ALTER TABLE alerts ADD COLUMN status TEXT NOT NULL DEFAULT 'open' "
        "CHECK (status IN ('open', 'acknowledged'))",
    ),
    ("acknowledged_by", "ALTER TABLE alerts ADD COLUMN acknowledged_by TEXT"),
    ("acknowledged_at", "ALTER TABLE alerts ADD COLUMN acknowledged_at REAL"),
)


def connect(db_path: str) -> sqlite3.Connection:
    # check_same_thread=False: FastAPI may run a request's dependency setup,
    # endpoint and teardown on different threadpool threads. Each connection
    # is still confined to a single request, and BEGIN IMMEDIATE serializes
    # writers, so this stays safe.
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate_alert_columns(conn)
        _migrate_rule_versioning(conn)
        # Safe now: a fresh table has version_id from SCHEMA, and an old one
        # gained it through _migrate_rule_versioning above.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_version ON alerts(version_id)"
        )
    finally:
        conn.close()


def _migrate_alert_columns(conn: sqlite3.Connection) -> None:
    """Add acknowledgement columns to a pre-workflow alerts table, once."""
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
    for name, ddl in ALERT_COLUMN_MIGRATIONS:
        if name not in existing:
            conn.execute(ddl)
    # Created only after the columns above certainly exist, so an old alerts
    # table migrated in the same startup cannot fail the index build.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_device_status ON alerts(device_id, status)"
    )


def _migrate_rule_versioning(conn: sqlite3.Connection) -> None:
    """Upgrade a pre-versioning database to the rule-series/versions schema.

    Detected by the old wide ``alert_rules`` shape (parameters lived on the
    rule row). Every old rule becomes version 1 effective from seq 0, its
    evaluation state is carried over, and historical alerts are pinned to that
    version. Runs once; a new-shape table is left untouched. The DDL rebuild
    happens with foreign-key enforcement off (SQLite cannot ALTER a referenced
    table otherwise) and ends with a foreign_key_check.
    """
    rule_cols = {r["name"] for r in conn.execute("PRAGMA table_info(alert_rules)")}
    if "field" not in rule_cols:
        return  # already the versioned shape (fresh SCHEMA) — nothing to do

    alert_cols = {r["name"] for r in conn.execute("PRAGMA table_info(alerts)")}
    if "version_id" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN version_id INTEGER")
    if "version_no" not in alert_cols:
        conn.execute("ALTER TABLE alerts ADD COLUMN version_no INTEGER")

    # A legacy database may have no alert_rule_state table at all (it predates
    # the state feature); SCHEMA then already created it in the new, versioned
    # shape, so there is no rule_id-keyed state to carry over. Only rebuild it
    # when the pre-migration table actually has the old shape.
    state_cols = {r["name"] for r in conn.execute("PRAGMA table_info(alert_rule_state)")}
    has_legacy_state = "rule_id" in state_cols

    # Rebuilding alert_rules/alert_rule_state while alerts and versions
    # reference them requires FK enforcement to be temporarily suspended.
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Defensive: an alert may point at a rule row that never existed
            # (hand-crafted/old volume). Reconstruct a placeholder series from
            # the alert's own snapshot so every alert can be pinned to a v1.
            conn.execute(
                "INSERT INTO alert_rules (id, device_id, field, op, threshold, "
                "consecutive, created_at) "
                "SELECT a.rule_id, a.device_id, a.field, a.op, a.threshold, "
                "a.consecutive, MIN(a.created_at) "
                "FROM alerts a LEFT JOIN alert_rules r ON r.id = a.rule_id "
                "WHERE r.id IS NULL GROUP BY a.rule_id"
            )

            old_rules = conn.execute(
                "SELECT id, device_id, field, op, threshold, consecutive, created_at "
                "FROM alert_rules"
            ).fetchall()
            for r in old_rules:
                cur = conn.execute(
                    "INSERT INTO alert_rule_versions (rule_id, version_no, field, "
                    "op, threshold, consecutive, effective_seq, created_at) "
                    "VALUES (?, 1, ?, ?, ?, ?, 0, ?)",
                    (r["id"], r["field"], r["op"], r["threshold"], r["consecutive"],
                     r["created_at"]),
                )
                conn.execute(
                    "UPDATE alerts SET version_id = ?, version_no = 1 WHERE rule_id = ?",
                    (cur.lastrowid, r["id"]),
                )

            # Rebuild state keyed by version_id instead of rule_id.
            if has_legacy_state:
                conn.execute(
                    "CREATE TABLE _ars_new ("
                    "version_id INTEGER PRIMARY KEY "
                    "REFERENCES alert_rule_versions (id) ON DELETE CASCADE, "
                    "last_seq INTEGER, streak INTEGER NOT NULL DEFAULT 0, "
                    "alerted INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL)"
                )
                conn.execute(
                    "INSERT INTO _ars_new (version_id, last_seq, streak, alerted, "
                    "updated_at) "
                    "SELECT v.id, s.last_seq, s.streak, s.alerted, s.updated_at "
                    "FROM alert_rule_state s "
                    "JOIN alert_rule_versions v ON v.rule_id = s.rule_id "
                    "AND v.version_no = 1"
                )
                conn.execute("DROP TABLE alert_rule_state")
                conn.execute("ALTER TABLE _ars_new RENAME TO alert_rule_state")
            else:
                # Fresh state rows for the migrated v1 versions.
                now_v1 = time.time()
                conn.execute(
                    "INSERT INTO alert_rule_state (version_id, last_seq, streak, "
                    "alerted, updated_at) SELECT id, NULL, 0, 0, ? "
                    "FROM alert_rule_versions",
                    (now_v1,),
                )

            # Rebuild alert_rules to the narrow series shape.
            conn.execute(
                "CREATE TABLE _ar_new ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "device_id TEXT NOT NULL, created_at REAL NOT NULL)"
            )
            conn.execute(
                "INSERT INTO _ar_new (id, device_id, created_at) "
                "SELECT id, device_id, created_at FROM alert_rules"
            )
            conn.execute("DROP TABLE alert_rules")
            conn.execute("ALTER TABLE _ar_new RENAME TO alert_rules")

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alert_rules_device ON alert_rules(device_id)"
            )
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")

    issues = conn.execute("PRAGMA foreign_key_check").fetchall()
    if issues:  # pragma: no cover - defensive guard for the rebuild above
        raise RuntimeError(f"rule-versioning migration left FK violations: {issues}")


@contextmanager
def write_txn(conn: sqlite3.Connection):
    """Exclusive write transaction; serializes writers at the SQLite level."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
