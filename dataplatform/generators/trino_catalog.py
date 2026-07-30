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
