from __future__ import annotations

import json
from datetime import datetime, timezone

from .registry import REPO_ROOT

# Sổ ghi những gì control plane ĐÃ TẠO trên engine. Đây là thứ cho phép deployer biết
# "cái nào đã bị gỡ khỏi metadata mà vẫn còn sống ngoài kia" — nửa còn thiếu để
# `metadata/` quyết định được cả cái KHÔNG được tồn tại (ADR-0045).
#
# KHÔNG commit: mỗi môi trường một bản, giống state của Terraform. Commit vào thì máy
# khác `apply` sẽ tưởng mình đã tạo những thứ chưa hề tạo, và xoá nhầm.
STATE_PATH = REPO_ROOT / ".platform-state.json"


def _read() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        # State hỏng thì coi như chưa có: mất khả năng GC một lần, còn hơn nổ giữa
        # chừng lúc đang apply.
        return {}
    return data if isinstance(data, dict) else {}


def load(key: str, default=None):
    """Giá trị đã ghi cho `key`, hoặc `default` nếu chưa có."""
    return _read().get(key, default)


def save(key: str, value) -> None:
    """Ghi `key`, giữ nguyên mọi key khác.

    Đọc-sửa-ghi cả file thay vì ghi đè: nhiều deployer cùng dùng một sổ, ghi đè sẽ làm
    deployer sau xoá mất state của deployer trước.
    """
    data = _read()
    data[key] = value
    data["_updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    STATE_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
                          encoding="utf-8", newline="\n")
