from __future__ import annotations

import os
import psycopg2
import psycopg2.extras
import config
from utils.settings_defaults import DEFAULT_SETTINGS

PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB", "tg_dashboard")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

# Alias untuk kompatibilitas kode lama yang import DB_FILE
DB_FILE = f"postgresql://{PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}"

config.DATA_DIR.mkdir(parents=True, exist_ok=True)
config.SESSION_DIR.mkdir(parents=True, exist_ok=True)


def _adapt_sql(sql):
    result = []
    in_string = False
    quote_char = None
    i = 0
    while i < len(sql):
        c = sql[i]
        if in_string:
            result.append(c)
            if c == quote_char and (i == 0 or sql[i-1] != chr(92)):
                in_string = False
        elif c in (chr(39), chr(34)):
            in_string = True
            quote_char = c
            result.append(c)
        elif c == chr(63):
            result.append("%s")
        else:
            result.append(c)
        i += 1
    return "".join(result)


class _DictRow:
    def __init__(self, data):
        self._data = dict(data) if data else {}
    def __getitem__(self, key):
        if isinstance(key, int):
            # Support akses via index angka seperti sqlite3.Row
            vals = list(self._data.values())
            if key < len(vals):
                return vals[key]
            raise IndexError(key)
        return self._data[key]
    def __contains__(self, key):
        return key in self._data
    def keys(self):
        return self._data.keys()
    def get(self, key, default=None):
        return self._data.get(key, default)
    def __iter__(self):
        return iter(self._data)
    def items(self):
        return self._data.items()
    def values(self):
        return self._data.values()
    def __repr__(self):
        return repr(self._data)


class _CursorWrapper:
    def __init__(self, cur):
        self._cur = cur
        self._last_id = None

    def execute(self, sql, params=None):
        sql = _adapt_sql(sql)
        if params is not None:
            self._cur.execute(sql, list(params))
        else:
            self._cur.execute(sql)
        return self

    def executemany(self, sql, seq):
        sql = _adapt_sql(sql)
        self._cur.executemany(sql, [list(p) for p in seq])

    def fetchone(self):
        row = self._cur.fetchone()
        return _DictRow(row) if row else None

    def fetchall(self):
        return [_DictRow(r) for r in (self._cur.fetchall() or [])]

    @property
    def rowcount(self):
        return self._cur.rowcount

    @property
    def lastrowid(self):
        try:
            row = self._cur.fetchone()
            if row:
                vals = list(dict(row).values())
                return vals[0] if vals else None
        except Exception:
            return None

    def __iter__(self):
        for row in self._cur:
            yield _DictRow(row)


class _ConnWrapper:
    def __init__(self, pg_conn):
        self._conn = pg_conn

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        wrapped = _CursorWrapper(cur)
        wrapped.execute(sql, params)
        return wrapped

    def executemany(self, sql, seq):
        cur = self._conn.cursor()
        wrapped = _CursorWrapper(cur)
        wrapped.executemany(sql, seq)
        return wrapped

    def cursor(self):
        return _CursorWrapper(self._conn.cursor())

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


def get_conn():
    pg = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
        connect_timeout=10,
        options="-c statement_timeout=60000",
    )
    pg.autocommit = False
    pg.cursor_factory = psycopg2.extras.RealDictCursor
    return _ConnWrapper(pg)


def _ensure_column(cur, table, column, sql_type, default_sql=""):
    type_map = {"INTEGER": "INTEGER", "TEXT": "TEXT", "REAL": "REAL", "BLOB": "BYTEA"}
    pg_type = type_map.get(sql_type.upper().split()[0], "TEXT")
    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s",
                (table, column))
    if cur.fetchone():
        return
    default_clause = ""
    if default_sql:
        d = default_sql.strip()
        if "datetime" in d:
            d = "TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')"
        default_clause = f" DEFAULT {d}"
    cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {pg_type}{default_clause}")



def _ensure_column(cur, table, column, sql_type, default_sql=""):
    type_map = {"INTEGER": "INTEGER", "TEXT": "TEXT", "REAL": "REAL", "BLOB": "BYTEA"}
    pg_type = type_map.get(sql_type.upper().split()[0], "TEXT")
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s",
        (table, column)
    )
    if cur.fetchone():
        return
    default_clause = ""
    if default_sql:
        d = default_sql.strip()
        if "datetime" in d:
            d = "TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')"
        default_clause = f" DEFAULT {d}"
    cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {pg_type}{default_clause}")


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS akun (
            phone TEXT PRIMARY KEY,
            nama TEXT, username TEXT,
            status TEXT DEFAULT 'active',
            tanggal_buat TEXT,
            level_warming INTEGER DEFAULT 1,
            score INTEGER DEFAULT 0,
            total_kirim INTEGER DEFAULT 0,
            total_berhasil INTEGER DEFAULT 0,
            total_flood INTEGER DEFAULT 0,
            total_banned INTEGER DEFAULT 0,
            dibuat TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            role TEXT DEFAULT 'hybrid',
            pool TEXT DEFAULT 'default',
            health_score INTEGER DEFAULT 100,
            daily_new_group_cap INTEGER DEFAULT 60,
            daily_send_cap INTEGER DEFAULT 20,
            concurrent_cap INTEGER DEFAULT 3,
            priority_weight INTEGER DEFAULT 100,
            auto_assign_enabled INTEGER DEFAULT 1,
            auto_send_enabled INTEGER DEFAULT 1,
            cooldown_until TEXT,
            last_login_at TEXT,
            last_activity_at TEXT,
            last_error_code TEXT,
            last_error_message TEXT,
            manual_health_override_enabled INTEGER DEFAULT 0,
            manual_health_override_score INTEGER DEFAULT 80,
            manual_warming_override_enabled INTEGER DEFAULT 0,
            manual_warming_override_level INTEGER DEFAULT 2,
            fresh_login_grace_enabled INTEGER DEFAULT 1,
            fresh_login_grace_minutes INTEGER DEFAULT 180,
            fresh_login_health_floor INTEGER DEFAULT 80,
            fresh_login_warming_floor INTEGER DEFAULT 2,
            assignment_notes TEXT,
            next_join_at TEXT,
            next_broadcast_at TEXT,
            last_broadcast_at TEXT,
            last_broadcast_group TEXT,
            last_join_at TEXT,
            last_join_group TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS grup (
            id BIGINT PRIMARY KEY,
            nama TEXT, username TEXT, tipe TEXT,
            jumlah_member INTEGER DEFAULT 0,
            link TEXT, status TEXT DEFAULT 'active',
            score INTEGER DEFAULT 0,
            label TEXT DEFAULT 'Normal',
            last_chat TEXT, last_kirim TEXT,
            total_kirim INTEGER DEFAULT 0,
            total_berhasil INTEGER DEFAULT 0,
            sumber TEXT DEFAULT 'fetch',
            aktif_indikator TEXT DEFAULT 'unknown',
            diupdate TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            permission_status TEXT DEFAULT 'unknown',
            eligibility_status TEXT DEFAULT 'eligible',
            assignment_status TEXT DEFAULT 'ready_assign',
            broadcast_status TEXT DEFAULT 'hold',
            owner_phone TEXT, notes TEXT,
            source_keyword TEXT, permission_basis TEXT,
            approved_by TEXT, approved_at TEXT,
            permission_expires_at TEXT,
            send_guard_status TEXT DEFAULT 'unknown',
            send_guard_reason TEXT,
            send_guard_checked_at TEXT,
            idle_days INTEGER DEFAULT 0,
            broadcast_ready_at TEXT,
            broadcast_hold_reason TEXT,
            join_ready_at TEXT, join_hold_reason TEXT,
            join_status TEXT, join_attempt_count INTEGER DEFAULT 0,
            broadcast_putaran_urut INTEGER DEFAULT 0,
            is_forum INTEGER DEFAULT 0,
            broadcast_attempt_count INTEGER DEFAULT 0,
            broadcast_last_sender TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS akun_grup (
            phone TEXT NOT NULL,
            grup_id BIGINT NOT NULL,
            dibuat TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            PRIMARY KEY (phone, grup_id),
            FOREIGN KEY (phone) REFERENCES akun(phone) ON DELETE CASCADE,
            FOREIGN KEY (grup_id) REFERENCES grup(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS draft (
            id SERIAL PRIMARY KEY,
            judul TEXT NOT NULL, isi TEXT NOT NULL,
            aktif INTEGER DEFAULT 0,
            dibuat TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS antrian (
            id SERIAL PRIMARY KEY,
            phone TEXT NOT NULL, grup_id BIGINT NOT NULL,
            pesan TEXT NOT NULL, status TEXT DEFAULT 'menunggu',
            dibuat TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            dikirim TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS riwayat (
            id SERIAL PRIMARY KEY,
            phone TEXT, grup_id BIGINT NOT NULL,
            nama_grup TEXT, status TEXT NOT NULL,
            pesan_error TEXT,
            waktu TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            label TEXT,
            tipe TEXT DEFAULT 'number'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_log (
            id SERIAL PRIMARY KEY,
            session_id TEXT NOT NULL,
            phone TEXT NOT NULL, grup_id BIGINT NOT NULL,
            nama_grup TEXT, status TEXT NOT NULL,
            pesan_error TEXT,
            waktu TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scrape_job (
            id SERIAL PRIMARY KEY,
            phone TEXT NOT NULL, keywords_text TEXT NOT NULL,
            options_json TEXT, status TEXT DEFAULT 'queued',
            total_keywords INTEGER DEFAULT 0,
            processed_keywords INTEGER DEFAULT 0,
            total_found INTEGER DEFAULT 0,
            total_saved INTEGER DEFAULT 0,
            total_imported INTEGER DEFAULT 0,
            error_message TEXT,
            dibuat TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            selesai TEXT, job_name TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scrape_keyword_run (
            id SERIAL PRIMARY KEY,
            job_id INTEGER NOT NULL, keyword TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            found_count INTEGER DEFAULT 0,
            saved_count INTEGER DEFAULT 0,
            error_message TEXT,
            started_at TEXT, finished_at TEXT,
            source TEXT DEFAULT 'base',
            priority INTEGER DEFAULT 50,
            tier TEXT DEFAULT 'medium',
            attempt_count INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 2,
            quality_score INTEGER DEFAULT 0,
            last_error_code TEXT DEFAULT '',
            UNIQUE(job_id, keyword),
            FOREIGN KEY (job_id) REFERENCES scrape_job(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scrape_result (
            id SERIAL PRIMARY KEY,
            job_id INTEGER NOT NULL, grup_id BIGINT NOT NULL,
            nama TEXT, username TEXT, tipe TEXT,
            jumlah_member INTEGER DEFAULT 0,
            link TEXT, deskripsi TEXT, sumber_keyword TEXT,
            relevance_score INTEGER DEFAULT 0,
            recommended INTEGER DEFAULT 0,
            already_in_db INTEGER DEFAULT 0,
            imported INTEGER DEFAULT 0,
            catatan TEXT, metadata TEXT,
            ditemukan TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            result_status TEXT DEFAULT 'new',
            quality_tier TEXT, reason_flags TEXT,
            UNIQUE(job_id, grup_id),
            FOREIGN KEY (job_id) REFERENCES scrape_job(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS group_permission (
            id SERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            permission_basis TEXT, approval_source TEXT,
            approved_by TEXT, approved_at TEXT, expires_at TEXT,
            status TEXT DEFAULT 'pending', notes TEXT,
            created_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            updated_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            FOREIGN KEY (group_id) REFERENCES grup(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS group_assignment (
            id SERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            assigned_account_id TEXT NOT NULL,
            assignment_type TEXT DEFAULT 'sync_owner',
            status TEXT DEFAULT 'queued',
            priority_level INTEGER DEFAULT 100,
            assign_reason TEXT, assign_score_snapshot TEXT,
            retry_count INTEGER DEFAULT 0, max_retry INTEGER DEFAULT 2,
            reassign_count INTEGER DEFAULT 0,
            assigned_at TEXT, last_attempt_at TEXT,
            next_retry_at TEXT, failure_reason TEXT, released_at TEXT,
            created_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            updated_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            FOREIGN KEY (group_id) REFERENCES grup(id) ON DELETE CASCADE,
            FOREIGN KEY (assigned_account_id) REFERENCES akun(phone) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS campaign (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT DEFAULT 'broadcast',
            status TEXT DEFAULT 'draft',
            target_mode TEXT DEFAULT 'rule_based',
            template_id INTEGER, sender_pool TEXT DEFAULT 'default',
            auto_start_enabled INTEGER DEFAULT 0,
            required_permission_status TEXT DEFAULT 'valid',
            required_group_status TEXT DEFAULT 'managed',
            total_targets INTEGER DEFAULT 0,
            eligible_targets INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            blocked_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            started_at TEXT, finished_at TEXT,
            session_key TEXT, session_status TEXT DEFAULT 'idle',
            session_started_at TEXT, session_finished_at TEXT,
            session_target_limit INTEGER DEFAULT 0, session_note TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS campaign_target (
            id SERIAL PRIMARY KEY,
            campaign_id INTEGER NOT NULL, group_id BIGINT NOT NULL,
            sender_account_id TEXT,
            status TEXT DEFAULT 'eligible',
            eligibility_reason TEXT, queue_position INTEGER,
            attempt_count INTEGER DEFAULT 0,
            last_attempt_at TEXT, next_attempt_at TEXT,
            delivery_result TEXT, failure_reason TEXT, blocked_reason TEXT,
            created_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            updated_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            hold_reason TEXT, staged_at TEXT, session_key TEXT,
            dispatch_slot INTEGER, reserved_at TEXT,
            finalized_at TEXT, last_outcome_code TEXT,
            FOREIGN KEY (campaign_id) REFERENCES campaign(id) ON DELETE CASCADE,
            FOREIGN KEY (group_id) REFERENCES grup(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS automation_rule (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL, rule_type TEXT NOT NULL,
            enabled INTEGER DEFAULT 1, priority INTEGER DEFAULT 100,
            condition_json TEXT, action_json TEXT,
            cooldown_seconds INTEGER DEFAULT 0, scope_json TEXT,
            last_triggered_at TEXT,
            success_count INTEGER DEFAULT 0, fail_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            updated_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS recovery_item (
            id SERIAL PRIMARY KEY,
            entity_type TEXT NOT NULL, entity_id TEXT NOT NULL,
            entity_name TEXT, current_status TEXT, worker_status TEXT,
            problem_type TEXT, severity TEXT DEFAULT 'medium',
            recovery_status TEXT DEFAULT 'recovery_needed',
            recovery_attempt_count INTEGER DEFAULT 0,
            last_activity_at TEXT, heartbeat_at TEXT,
            last_recovery_at TEXT, last_recovery_result TEXT, note TEXT,
            created_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS'),
            updated_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id SERIAL PRIMARY KEY,
            level TEXT DEFAULT 'info', module TEXT,
            entity_type TEXT, entity_id TEXT,
            action TEXT, result TEXT, message TEXT, payload TEXT,
            created_at TEXT DEFAULT TO_CHAR(NOW(),'YYYY-MM-DD HH24:MI:SS')
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_throttle (
            id INTEGER PRIMARY KEY DEFAULT 1,
            last_broadcast_at TEXT, next_allowed_at TEXT, putaran_seed TEXT,
            CONSTRAINT broadcast_throttle_single CHECK (id = 1)
        )
    """)
    cur.execute("INSERT INTO broadcast_throttle (id) VALUES (1) ON CONFLICT (id) DO NOTHING")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_throttle_akun (
            phone TEXT PRIMARY KEY,
            last_broadcast_at TEXT, next_allowed_at TEXT
        )
    """)

    for idx in [
        "CREATE INDEX IF NOT EXISTS idx_grup_status_score ON grup(status, score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_riwayat_phone_waktu ON riwayat(phone, waktu)",
        "CREATE INDEX IF NOT EXISTS idx_riwayat_grup_waktu ON riwayat(grup_id, waktu)",
        "CREATE INDEX IF NOT EXISTS idx_akun_grup_phone ON akun_grup(phone)",
        "CREATE INDEX IF NOT EXISTS idx_akun_grup_gid ON akun_grup(grup_id)",
        "CREATE INDEX IF NOT EXISTS idx_scrape_job_status ON scrape_job(status, dibuat DESC)",
        "CREATE INDEX IF NOT EXISTS idx_campaign_status ON campaign(status, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_campaign_target_campaign ON campaign_target(campaign_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_recovery_item_status ON recovery_item(recovery_status, severity)",
        "CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at DESC, module)",
    ]:
        cur.execute(idx)

    conn.commit()
    conn.close()
    _init_settings_default()
    print("Database PostgreSQL siap:", PG_DB)


def _init_settings_default():
    from utils.settings_defaults import DEFAULT_SETTINGS
    conn = get_conn()
    for key, value, label, tipe in DEFAULT_SETTINGS:
        conn.execute(
            "INSERT INTO settings (key, value, label, tipe) VALUES (%s, %s, %s, %s) ON CONFLICT (key) DO NOTHING",
            (key, value, label, tipe)
        )
    conn.commit()
    conn.close()
