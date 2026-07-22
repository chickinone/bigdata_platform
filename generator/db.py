import psycopg2
from config import Config


def connect():
    """Tạo connection. autocommit=False để control transaction tường minh."""
    conn = psycopg2.connect(
        host=Config.PG_HOST,
        port=Config.PG_PORT,
        dbname=Config.PG_DB,
        user=Config.PG_USER,
        password=Config.PG_PASSWORD,
    )
    conn.autocommit = False
    return conn


def load_accounts(conn):
    """
    Load tất cả active accounts vào memory.
    Trả về list of dict để pick random nhanh.

    không load frozen/closed accounts — app thật không tạo giao dịch
    trên account inactive.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT account_id, customer_id, currency, balance
        FROM accounts
        WHERE status = 'active'
    """)
    rows = cur.fetchall()
    cur.close()

    if not rows:
        raise RuntimeError(
            "No active accounts found! Đã chạy 05_seed_data.sql chưa?"
        )

    return [
        {
            "id": r[0],
            "customer_id": r[1],
            "currency": r[2],
            "balance": r[3],  
        }
        for r in rows
    ]
