"""Đọc và validate các dataset contract trong metadata/.

Đây là tầng thấp nhất của control plane. Mọi generator đều đi qua đây, nên
contract sai sẽ bị chặn ở MỘT chỗ thay vì làm hỏng từng generator một cách
khác nhau.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parent.parent
METADATA_DIR = REPO_ROOT / "metadata"
SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"


class ContractError(Exception):
    """Contract không hợp lệ. Dừng hẳn — không sinh artifact từ metadata sai."""


@dataclass(frozen=True)
class Dataset:
    """Một contract đã được validate, bọc lại cho generator dùng.

    Các thuộc tính suy diễn (entity, topic, is_cdc...) sống ở đây chứ không nằm
    rải trong từng generator — nếu không, mỗi generator sẽ tự suy một kiểu và ta
    lại tạo ra đúng thứ sprawl mà cả dự án đang muốn diệt.
    """

    raw: dict
    path: Path

    @property
    def urn(self) -> str:
        return self.raw["urn"]

    @property
    def source_type(self) -> str:
        return self.raw["source"]["type"]

    @property
    def topic(self) -> str:
        return self.raw["source"]["topic"]

    @property
    def is_cdc(self) -> bool:
        return self.source_type == "cdc_debezium"

    @property
    def primary_key(self) -> str | None:
        return self.raw.get("primary_key")

    @property
    def entity(self) -> str:
        """Tên ngắn dùng để đặt tên artifact (connector, index...).

        CDC lấy tên bảng; stream lấy tên topic. Quy ước này bị khoá ở một chỗ
        duy nhất, nên đổi quy ước = sửa một hàm.
        """
        if self.is_cdc:
            return self.raw["source"]["table"]
        return self.topic

    def sink_enabled(self, name: str) -> bool:
        return self.raw.get("sinks", {}).get(name, {}).get("enabled", False)

    def columns(self) -> list[dict]:
        return self.raw.get("columns", [])


def _load_schema() -> dict:
    return json.loads((SCHEMA_DIR / "dataset.schema.json").read_text(encoding="utf-8"))


def load_datasets(metadata_dir: Path = METADATA_DIR) -> list[Dataset]:
    """Đọc mọi contract, validate theo JSON Schema, trả về danh sách đã sắp xếp.

    Sắp xếp theo urn để output của generator ổn định giữa các lần chạy — điều
    kiện bắt buộc để `--check` có nghĩa (diff phải phản ánh thay đổi thật, không
    phải thứ tự file ngẫu nhiên của hệ điều hành).
    """
    validator = Draft202012Validator(_load_schema())
    datasets: list[Dataset] = []
    errors: list[str] = []

    for path in sorted((metadata_dir / "datasets").rglob("*.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        rel = path.relative_to(REPO_ROOT)

        for err in sorted(validator.iter_errors(raw), key=lambda e: list(e.path)):
            loc = ".".join(str(p) for p in err.path) or "(gốc)"
            errors.append(f"  {rel} -> {loc}: {err.message}")

        if raw is not None:
            datasets.append(Dataset(raw=raw, path=path))

    if errors:
        raise ContractError(
            "Contract không hợp lệ:\n" + "\n".join(errors)
        )

    _check_unique_urns(datasets)
    return sorted(datasets, key=lambda d: d.urn)


def _check_unique_urns(datasets: list[Dataset]) -> None:
    """URN trùng nhau là lỗi chí mạng: hai contract cùng mô tả một thực thể thì
    generator sẽ ghi đè artifact của nhau một cách không xác định. JSON Schema
    không bắt được lỗi này vì nó chỉ nhìn từng file riêng lẻ.
    """
    seen: dict[str, Path] = {}
    for ds in datasets:
        if ds.urn in seen:
            raise ContractError(
                f"URN trùng: {ds.urn}\n  {seen[ds.urn]}\n  {ds.path}"
            )
        seen[ds.urn] = ds.path
