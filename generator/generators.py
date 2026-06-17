import random
import time
from decimal import Decimal
from faker import Faker

from config import Config

_fake = Faker()
_MERCHANT_NAMES = [_fake.company() for _ in range(200)]
_MERCHANT_CATEGORIES = [
    "groceries", "gas", "restaurant", "retail", "travel",
    "subscription", "utilities", "entertainment", "healthcare", "education",
]
_FAILURE_REASONS = [
    "insufficient_funds", "account_frozen", "limit_exceeded",
    "invalid_destination", "compliance_hold", "timeout",
]


def _to_decimal(value):
    """Convert float → Decimal cho NUMERIC(19,4) column. 2 chữ số thập phân là đủ."""
    return Decimal(str(round(value, 2)))


def _pick_simple_txn_type():
    """Distribution thực tế của giao dịch non-transfer (cộng dồn = 1.0)."""
    r = random.random()
    if r < 0.35:
        return "withdrawal"
    elif r < 0.65:
        return "deposit"
    elif r < 0.85:
        return "fee"
    elif r < 0.97:
        return "interest"
    else:
        return random.choice(["transfer_in", "transfer_out"])


def _pick_amount(txn_type):
    if txn_type == "fee":
        return _to_decimal(random.uniform(0.50, 25.0))
    elif txn_type == "interest":
        return _to_decimal(random.uniform(0.01, 50.0))
    elif txn_type == "deposit":
        # median ~ exp(4.5) ≈ $90, có thể lên $10k+
        return _to_decimal(random.lognormvariate(4.5, 1.2))
    else:  # withdrawal, transfer_in, transfer_out
        return _to_decimal(random.lognormvariate(4.0, 1.0))


# CREATE TRANSACTION (simple deposit/withdrawal/fee/interest)
def create_transaction(conn, accounts):
    """
    Atomic: UPDATE balance + INSERT transaction trong 1 BEGIN..COMMIT.
    
    Nếu fail (vd: CHECK constraint amount > 0) → rollback, cache không update,
    trả False. Realistic: app thật cũng có business rule reject.
    """
    acc = random.choice(accounts)
    txn_type = _pick_simple_txn_type()
    amount = _to_decimal(float(_pick_amount(txn_type)))

    # Direction: money in vs out
    if txn_type in ("deposit", "interest", "transfer_in"):
        new_balance = acc["balance"] + amount
    else:
        new_balance = acc["balance"] - amount

    # Merchant chỉ cho withdrawal (mô phỏng mua hàng)
    merchant_name = random.choice(_MERCHANT_NAMES) if txn_type == "withdrawal" else None
    merchant_cat = random.choice(_MERCHANT_CATEGORIES) if merchant_name else None
    description = f"{txn_type.capitalize()}"
    if merchant_name:
        description += f" at {merchant_name}"

    cur = conn.cursor()
    # Decide failure trước (5% rate, dùng cùng config với transfer cho nhất quán)
    will_fail = random.random() < 0.05
    
    cur = conn.cursor()
    try:
        if will_fail:
            # FAIL case: INSERT transaction status='failed', balance giữ nguyên
            # Mô phỏng business reject (insufficient funds, frozen account, ...)
            failure_desc = description + f" — {random.choice(_FAILURE_REASONS)}"
            cur.execute(
                """
                INSERT INTO transactions
                    (account_id, transaction_type, amount, balance_after, currency,
                     merchant_name, merchant_category, description, status, posted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'failed', NOW())
                """,
                (acc["id"], txn_type, amount, acc["balance"], acc["currency"],
                 merchant_name, merchant_cat, failure_desc),
            )
            conn.commit()
            # KHÔNG update acc["balance"] vì transaction không thực sự diễn ra
            return True

        # SUCCESS case (logic cũ giữ nguyên)
        # Step 1: UPDATE balance
        cur.execute(
            "UPDATE accounts SET balance = %s WHERE account_id = %s",
            (new_balance, acc["id"]),
        )
        # Step 2: INSERT transaction
        cur.execute(
            """
            INSERT INTO transactions
                (account_id, transaction_type, amount, balance_after, currency,
                 merchant_name, merchant_category, description, status, posted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'completed', NOW())
            """,
            (acc["id"], txn_type, amount, new_balance, acc["currency"],
             merchant_name, merchant_cat, description),
        )
        conn.commit()

        acc["balance"] = new_balance
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        cur.close()


# ============================================================
# TRANSFER LIFECYCLE — state machine
# ============================================================
def initiate_transfer(conn, accounts):
    """
    Tạo transfer với status='pending'. CHƯA update balance.
    Trả về dict để main loop track và finalize sau.
    """
    from_acc = random.choice(accounts)

    # Pick to_account khác from, cùng currency
    same_curr = [
        a for a in accounts
        if a["currency"] == from_acc["currency"] and a["id"] != from_acc["id"]
    ]
    if not same_curr:
        return None
    to_acc = random.choice(same_curr)

    amount = _pick_amount("transfer_out")
    # Tránh transfer vượt balance (sẽ fail CHECK của app banking)
    if amount > from_acc["balance"]:
        amount = _to_decimal(float(from_acc["balance"]) * random.uniform(0.05, 0.3))
        if amount <= 0:
            return None

    # Reference code unique - mô phỏng "trace ID" thực tế
    ref_code = f"TRF-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"

    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO transfers
                (from_account_id, to_account_id, amount, currency, status, reference_code)
            VALUES (%s, %s, %s, %s, 'pending', %s)
            RETURNING transfer_id
            """,
            (from_acc["id"], to_acc["id"], amount, from_acc["currency"], ref_code),
        )
        transfer_id = cur.fetchone()[0]
        conn.commit()

        # Quyết định trước transfer sẽ fail hay thành công (deterministic cho debug)
        return {
            "transfer_id": transfer_id,
            "from_acc": from_acc,
            "to_acc": to_acc,
            "amount": amount,
            "currency": from_acc["currency"],
            "complete_at": time.time() + random.uniform(
                Config.TRANSFER_DELAY_MIN, Config.TRANSFER_DELAY_MAX
            ),
            "should_fail": random.random() < Config.PROB_FAILURE,
        }
    except Exception:
        conn.rollback()
        return None
    finally:
        cur.close()


def finalize_transfer(conn, t):
    """
    Hoàn tất transfer qua 2 state transition:
        pending → processing → (completed | failed)
    
    Mỗi state transition là 1 UPDATE riêng (commit riêng) → Debezium thấy
    nhiều events. Đây là cơ chế Flink CEP detect state pattern sau này.
    
    Trả về: 'completed' | 'failed' | 'error'
    """
    cur = conn.cursor()
    try:
        # ----- State 1: pending → processing -----
        cur.execute(
            "UPDATE transfers SET status = 'processing' WHERE transfer_id = %s",
            (t["transfer_id"],),
        )
        conn.commit()

        # ----- State 2a: → failed -----
        if t["should_fail"]:
            cur.execute(
                """
                UPDATE transfers
                SET status = 'failed', failure_reason = %s, completed_at = NOW()
                WHERE transfer_id = %s
                """,
                (random.choice(_FAILURE_REASONS), t["transfer_id"]),
            )
            conn.commit()
            return "failed"

        # ----- State 2b: → completed (cả block phải atomic) -----
        new_from = t["from_acc"]["balance"] - t["amount"]
        new_to = t["to_acc"]["balance"] + t["amount"]

        cur.execute(
            "UPDATE accounts SET balance = %s WHERE account_id = %s",
            (new_from, t["from_acc"]["id"]),
        )
        cur.execute(
            "UPDATE accounts SET balance = %s WHERE account_id = %s",
            (new_to, t["to_acc"]["id"]),
        )

        # 2 transactions ghi nhận money out / money in
        cur.execute(
            """
            INSERT INTO transactions
                (account_id, transaction_type, amount, balance_after,
                 currency, description, status, posted_at)
            VALUES (%s, 'transfer_out', %s, %s, %s, %s, 'completed', NOW())
            """,
            (t["from_acc"]["id"], t["amount"], new_from, t["currency"],
             f"Transfer to account {t['to_acc']['id']}"),
        )
        cur.execute(
            """
            INSERT INTO transactions
                (account_id, transaction_type, amount, balance_after,
                 currency, description, status, posted_at)
            VALUES (%s, 'transfer_in', %s, %s, %s, %s, 'completed', NOW())
            """,
            (t["to_acc"]["id"], t["amount"], new_to, t["currency"],
             f"Transfer from account {t['from_acc']['id']}"),
        )

        # Cuối cùng: mark transfer completed
        cur.execute(
            """
            UPDATE transfers
            SET status = 'completed', completed_at = NOW()
            WHERE transfer_id = %s
            """,
            (t["transfer_id"],),
        )
        # Commit cả block balance updates + 2 transactions + transfer completion
        conn.commit()

        t["from_acc"]["balance"] = new_from
        t["to_acc"]["balance"] = new_to
        return "completed"
    except Exception:
        conn.rollback()
        return "error"
    finally:
        cur.close()
