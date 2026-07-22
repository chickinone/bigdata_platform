"""Sinh Trino catalog properties từ connection registry — Pha 6 (federation).

Trước đây `trino/etc/catalog/*.properties` viết tay, tách rời khỏi định nghĩa
connection (sprawl #13). Nay sinh từ connection contract: mỗi connection có khối
`trino` → một file catalog. Thêm nguồn cho Trino = thêm khối `trino` vào connection,
không sửa file .properties tay.

Secret không nằm ở đây: mọi giá trị nhạy cảm là `${ENV:...}` để Trino tự resolve
(cùng nguyên tắc với connector/deployer).
"""
from __future__ import annotations


def trino_connections(connections: list[dict]) -> list[dict]:
    members = [c for c in connections if c.get("trino")]
    return sorted(members, key=lambda c: c["trino"]["catalog"])


def render(conn: dict) -> str:
    """Một file .properties: connector.name trước, rồi các property theo thứ tự khai.

    Thứ tự giữ đúng như khai trong YAML (dict giữ thứ tự chèn) để diff byte-exact với
    bản viết tay có nghĩa.
    """
    t = conn["trino"]
    lines = [f"connector.name={t['connector']}"]
    lines += [f"{k}={v}" for k, v in t["properties"].items()]
    return "\n".join(lines) + "\n"


def targets(connections: list[dict]) -> dict[str, str]:
    out: dict[str, str] = {}
    for conn in trino_connections(connections):
        out[f"trino/etc/catalog/{conn['trino']['catalog']}.properties"] = render(conn)
    return out
