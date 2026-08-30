from __future__ import annotations

import subprocess
import sys

from ..generators.trino_catalog import trino_connections
from ..registry import ContractError, load_connections

CONTAINER = "bigdata-trino"


def _trino(sql: str) -> list[str]:
    """Chay SQL trong container Trino, tra ve cac dong da bo dau nháy.

    Dung `docker exec` thay vi client Python: khong them phu thuoc, va credential (neu
    sau nay bat auth) nam trong container chu khong phai o day — cung ly do da ap cho
    `_psql` trong postgres_schema (ADR-0041).
    """
    proc = subprocess.run(
        ["docker", "exec", CONTAINER, "trino", "--output-format", "TSV", "--execute", sql],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout).strip()[:400])
    return [ln.strip().strip('"') for ln in proc.stdout.splitlines() if ln.strip()]


def cmd_verify() -> int:
    conns = trino_connections(load_connections())
    want = {c["trino"]["catalog"]: c for c in conns}

    live = set(_trino("SHOW CATALOGS"))
    errors: list[str] = []
    notes: list[str] = []

    print(f"Doi chieu {len(want)} catalog khai trong connection registry voi Trino dang chay:\n")

    for name in sorted(want):
        if name not in live:
            errors.append(f"catalog `{name}` KHONG ton tai trong Trino (chua mount .properties? chua restart?)")
            print(f"  [THIEU] {name}")
            continue
        # Ton tai chua du: file .properties hong (vd CRLF, sai endpoint) van tao ra
        # catalog nhung query se no. Phai cham that moi biet.
        try:
            _trino(f"SHOW SCHEMAS FROM {name}")
            print(f"  [OK   ] {name}")
        except RuntimeError as exc:
            errors.append(f"catalog `{name}` co mat nhung KHONG query duoc: {exc}")
            print(f"  [LOI  ] {name}")

    for name in sorted(live - set(want) - {"system"}):
        notes.append(f"catalog `{name}` co trong Trino nhung KHONG khai trong registry (drift thu cong?)")

    # Healthcheck cua image la bash va doc .properties bang `cut` — CRLF bien port thanh
    # "8080\r" va container bao unhealthy VINH VIEN du Trino khoe (ADR-0043). Kiem o day
    # vi khong cong nao khac nhin thay: config.properties la file VIET TAY, khong nam
    # trong 19 artifact nen `cli check` khong phu.
    try:
        health = subprocess.run(
            ["docker", "inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}", CONTAINER],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        ).stdout.strip()
        if health == "unhealthy":
            errors.append(
                "container Trino bao `unhealthy` trong khi query chay duoc — gan nhu chac chan "
                "la CRLF trong trino/etc/*.properties (xem ADR-0043). Kiem bang: "
                "docker inspect -f '{{range .State.Health.Log}}{{.Output}}{{end}}' " + CONTAINER
            )
            print("  [LOI  ] healthcheck container = unhealthy")
        elif health:
            print(f"\n  healthcheck container: {health}")
    except OSError:
        pass

    print()
    for n in notes:
        print(f"  [chu y] {n}")
    for e in errors:
        print(f"  [LECH ] {e}")

    print()
    if errors:
        print(f"KET QUA: {len(errors)} lech, {len(notes)} chu y.")
        return 1
    print(f"KET QUA: 0 lech, {len(notes)} chu y. Moi catalog khai trong registry deu query duoc.")
    return 0


def _force_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    _force_utf8()
    try:
        return cmd_verify()
    except ContractError as exc:
        print(f"LOI CONTRACT\n{exc}", file=sys.stderr)
        return 2
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"KHONG doi chieu duoc voi Trino: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
