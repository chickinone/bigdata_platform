
import random
import signal
import sys
import time

from config import Config
from db import connect, load_accounts
from generators import create_transaction, initiate_transfer, finalize_transfer


class DataGenerator:
    def __init__(self):
        self.conn = connect()
        self.accounts = load_accounts(self.conn)
        print(f"[INIT] Loaded {len(self.accounts)} active accounts")
        print(f"[INIT] Target {Config.TARGET_RPS} RPS, peak {Config.PEAK_RPS} RPS, "
              f"duration {Config.DURATION_SEC}s ({Config.DURATION_SEC // 60} min)")

        self.pending_transfers = []
        self.stats = {
            "tx_ok": 0,
            "tx_rejected": 0,
            "transfer_initiated": 0,
            "transfer_completed": 0,
            "transfer_failed": 0,
            "transfer_error": 0,
            "burst_count": 0,
        }
        self.shutdown = False

        # Graceful shutdown on SIGINT/SIGTERM
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

    def _on_signal(self, signum, frame):
        print(f"\n[SHUTDOWN] Got signal {signum}, finalizing pending work...")
        self.shutdown = True

    def _print_stats(self, elapsed):
        actual_rps = (self.stats["tx_ok"] + self.stats["transfer_completed"]
                      + self.stats["transfer_failed"]) / max(elapsed, 1)
        print(
            f"[{int(elapsed):4d}s] "
            f"tx_ok={self.stats['tx_ok']:6d} "
            f"tx_rej={self.stats['tx_rejected']:4d} "
            f"xfer={self.stats['transfer_initiated']:5d} "
            f"done={self.stats['transfer_completed']:5d} "
            f"fail={self.stats['transfer_failed']:4d} "
            f"err={self.stats['transfer_error']:3d} "
            f"pending={len(self.pending_transfers):3d} "
            f"actual_rps={actual_rps:.1f} "
            f"bursts={self.stats['burst_count']}"
        )

    def _process_pending(self, now):
        """Complete những transfer nào đã đến lúc (complete_at <= now)."""
        still_pending = []
        for t in self.pending_transfers:
            if now >= t["complete_at"]:
                result = finalize_transfer(self.conn, t)
                if result == "completed":
                    self.stats["transfer_completed"] += 1
                elif result == "failed":
                    self.stats["transfer_failed"] += 1
                else:
                    self.stats["transfer_error"] += 1
            else:
                still_pending.append(t)
        self.pending_transfers = still_pending

    def run(self):
        start = time.time()
        end = start + Config.DURATION_SEC
        burst_until = 0.0
        last_stats = start

        try:
            while time.time() < end and not self.shutdown:
                loop_start = time.time()

                # ----- Burst trigger check -----
                if loop_start > burst_until:
                    if random.random() < Config.BURST_PROBABILITY:
                        burst_len = random.uniform(2.0, Config.BURST_DURATION_MAX)
                        burst_until = loop_start + burst_len
                        self.stats["burst_count"] += 1
                        print(f"[{int(loop_start - start):4d}s] >>> BURST for {burst_len:.1f}s")

                current_rps = (Config.PEAK_RPS if loop_start < burst_until
                               else Config.TARGET_RPS)
                interval = 1.0 / current_rps

                # ----- Sinh 1 unit of work -----
                if random.random() < Config.PROB_TRANSFER:
                    t = initiate_transfer(self.conn, self.accounts)
                    if t:
                        self.pending_transfers.append(t)
                        self.stats["transfer_initiated"] += 1
                else:
                    if create_transaction(self.conn, self.accounts):
                        self.stats["tx_ok"] += 1
                    else:
                        self.stats["tx_rejected"] += 1

                # ----- Process pending transfers (cheap, ~O(pending) -----
                self._process_pending(loop_start)

                # ----- Periodic stats output -----
                if loop_start - last_stats >= Config.STATS_INTERVAL_SEC:
                    self._print_stats(loop_start - start)
                    last_stats = loop_start

                # ----- Rate limit: sleep phần còn lại của interval -----
                elapsed_in_loop = time.time() - loop_start
                sleep_time = interval - elapsed_in_loop
                if sleep_time > 0:
                    time.sleep(sleep_time)
                # Nếu elapsed_in_loop > interval: machine không theo kịp target RPS,
                # cứ chạy hết tốc độ. Stats sẽ phản ánh actual RPS thấp hơn target.

        finally:
            # Drain pending transfers còn lại
            print(f"\n[DRAIN] Finalizing {len(self.pending_transfers)} pending transfers...")
            self._process_pending(time.time() + 100)  # force all to complete

            print("\n[FINAL STATS]")
            self._print_stats(time.time() - start)
            self.conn.close()
            print("[DONE]")


if __name__ == "__main__":
    try:
        DataGenerator().run()
    except Exception as e:
        print(f"[FATAL] {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
