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

