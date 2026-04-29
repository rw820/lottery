import logging
import os
import sqlite3
from typing import List, Optional

from .config import DB_PATH
from .models import SSQDraw, DLTDraw

logger = logging.getLogger(__name__)

SSQ_CREATE_SQL = """\
CREATE TABLE IF NOT EXISTS ssq_draws (
    code TEXT PRIMARY KEY,
    draw_date TEXT NOT NULL,
    week_day TEXT,
    red TEXT NOT NULL,
    blue TEXT NOT NULL,
    sales INTEGER,
    pool_money INTEGER,
    content TEXT,
    prize_grades TEXT
)
"""

DLT_CREATE_SQL = """\
CREATE TABLE IF NOT EXISTS dlt_draws (
    code TEXT PRIMARY KEY,
    draw_date TEXT NOT NULL,
    front TEXT NOT NULL,
    back TEXT NOT NULL,
    sales INTEGER,
    pool_money INTEGER,
    prize_grades TEXT
)
"""


def _get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = _get_conn(db_path)
    try:
        conn.execute(SSQ_CREATE_SQL)
        conn.execute(DLT_CREATE_SQL)
        # Add columns if they don't exist (for existing DBs)
        for sql in [
            "ALTER TABLE dlt_draws ADD COLUMN pool_money INTEGER",
            "ALTER TABLE dlt_draws ADD COLUMN prize_grades TEXT",
        ]:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                pass
        conn.commit()
        logger.info("Database initialized at %s", db_path)
    finally:
        conn.close()


def get_latest_code(lottery: str, db_path: str = DB_PATH) -> Optional[str]:
    table = "ssq_draws" if lottery == "ssq" else "dlt_draws"
    conn = _get_conn(db_path)
    try:
        row = conn.execute(f"SELECT MAX(code) FROM {table}").fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def get_draw_count(lottery: str, db_path: str = DB_PATH) -> int:
    table = "ssq_draws" if lottery == "ssq" else "dlt_draws"
    conn = _get_conn(db_path)
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return row[0]
    finally:
        conn.close()


def count_draws(
    lottery: str,
    code: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    db_path: str = DB_PATH,
) -> int:
    table = "ssq_draws" if lottery == "ssq" else "dlt_draws"
    conditions = []
    params = []
    if code:
        conditions.append("code LIKE ?")
        params.append(f"%{code}%")
    if date_start:
        conditions.append("draw_date >= ?")
        params.append(date_start)
    if date_end:
        conditions.append("draw_date <= ?")
        params.append(date_end)
    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    conn = _get_conn(db_path)
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}{where}", params).fetchone()
        return row[0]
    finally:
        conn.close()


def insert_ssq_draws(draws: List[SSQDraw], db_path: str = DB_PATH) -> int:
    conn = _get_conn(db_path)
    try:
        cur = conn.executemany(
            "INSERT OR REPLACE INTO ssq_draws (code, draw_date, week_day, red, blue, sales, pool_money, content, prize_grades) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(d.code, d.draw_date, d.week_day, d.red, d.blue, d.sales, d.pool_money, d.content, d.prize_grades) for d in draws],
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def insert_dlt_draws(draws: List[DLTDraw], db_path: str = DB_PATH) -> int:
    conn = _get_conn(db_path)
    try:
        cur = conn.executemany(
            "INSERT OR REPLACE INTO dlt_draws (code, draw_date, front, back, sales, pool_money, prize_grades) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(d.code, d.draw_date, d.front, d.back, d.sales, d.pool_money, d.prize_grades) for d in draws],
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def query_draws(
    lottery: str,
    code: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    limit: Optional[int] = None,
    order: str = "DESC",
    db_path: str = DB_PATH,
    offset: int = 0,
) -> List[dict]:
    table = "ssq_draws" if lottery == "ssq" else "dlt_draws"
    conditions = []
    params = []

    if code:
        conditions.append("code LIKE ?")
        params.append(f"%{code}%")
    if date_start:
        conditions.append("draw_date >= ?")
        params.append(date_start)
    if date_end:
        conditions.append("draw_date <= ?")
        params.append(date_end)

    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM {table}{where} ORDER BY code {order}"

    if limit:
        sql += f" LIMIT {int(limit)} OFFSET {int(offset)}"

    conn = _get_conn(db_path)
    try:
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()
