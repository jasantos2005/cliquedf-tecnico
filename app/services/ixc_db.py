import os, pymysql
from contextlib import contextmanager
from pymysql.cursors import DictCursor
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

def _cfg():
    return dict(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=10,
    )

@contextmanager
def ixc_conn():
    conn = pymysql.connect(**_cfg())
    try:
        cur = conn.cursor()
        cur.execute("SET SESSION time_zone = '-03:00'")
        yield conn
    finally:
        conn.close()

def ixc_select(sql, params=()):
    with ixc_conn() as c:
        cur = c.cursor(); cur.execute(sql, params); return cur.fetchall()

def ixc_select_one(sql, params=()):
    with ixc_conn() as c:
        cur = c.cursor(); cur.execute(sql, params); return cur.fetchone()

def ixc_insert(sql, params=()):
    with ixc_conn() as c:
        cur = c.cursor(); cur.execute(sql, params); c.commit(); return cur.lastrowid
