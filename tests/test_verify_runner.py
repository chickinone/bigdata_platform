from pathlib import Path

from dataplatform.cli import VERIFIERS

VERIFIERS_DIR = Path(__file__).resolve().parents[1] / "dataplatform" / "verifiers"


def _modules_on_disk():
    return sorted(
        p.stem for p in VERIFIERS_DIR.glob("*.py") if p.stem != "__init__"
    )


def test_danh_sach_khop_thu_muc():
    """`cli verify` phai chay DU moi verifier co that.

    Day dung la kieu sprawl du an nay chong: mot danh sach viet tay phai khop mot su
    that o cho khac. Them verifier moi ma quen them vao VERIFIERS thi no im lang khong
    bao gio chay — va ta se lai co "bo do ton tai nhung khong ai goi" (ADR-0043).
    """
    assert sorted(VERIFIERS) == _modules_on_disk()


def test_khong_trung_ten():
    assert len(VERIFIERS) == len(set(VERIFIERS))


def test_moi_verifier_co_main_tra_int():
    import importlib

    for name in VERIFIERS:
        mod = importlib.import_module(f"dataplatform.verifiers.{name}")
        assert hasattr(mod, "main"), f"{name} thieu main()"
