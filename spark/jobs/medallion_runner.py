# Python trong image Spark là 3.8: cú pháp `str | None` chỉ có từ 3.10. Future import
# biến annotation thành chuỗi lười nên file chạy được ở cả hai nơi (test chạy Python
# host mới hơn, runner chạy trong container cũ).
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

# pyspark import BÊN TRONG hàm, không phải đầu file: phần thuần Python dưới đây
# (toán cửa sổ + các bất biến) phải import được trong CI tĩnh, nơi không có Spark.


def build_spark(name: str, iceberg: bool):
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder
        .appName(name)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", os.getenv("S3_PATH_STYLE_ACCESS", "true"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", os.getenv("S3_SSL_ENABLED", "false"))
    )
    if iceberg:
        # Chỉ thêm khi output là iceberg (cần iceberg jar do deployer nạp riêng).
        # Cùng cấu hình đã chứng minh ở silver_to_iceberg.py: catalog REST + HadoopFileIO
        # (S3A battle-tested với MinIO, tránh S3FileIO hang multipart).
        b = (
            b.config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "rest")
            .config("spark.sql.catalog.lakehouse.uri", os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
            .config("spark.sql.catalog.lakehouse.warehouse", os.getenv("ICEBERG_WAREHOUSE", "s3a://data-lake-iceberg/warehouse"))
            .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
            .config("spark.sql.defaultCatalog", "lakehouse")
            # Server iceberg-rest trả path scheme s3:// (S3FileIO), nhưng client dùng
            # HadoopFileIO chỉ có s3a. Map s3 -> S3AFileSystem: S3A đọc config fs.s3a.*
            # sẵn có ở trên, nên endpoint/key MinIO áp dụng cho cả s3://.
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    return b.getOrCreate()


def as_of_date(raw: str | None = None) -> date:
    """Ngày mốc của lần chạy. Airflow truyền `{{ds}}` (đầu data interval) nên chạy lại
    một ngày cũ sẽ tính lại đúng cửa sổ ngày đó — backfill không cần cờ riêng."""
    raw = (os.getenv("AS_OF", "") if raw is None else raw).strip()
    if not raw:
        return date.today()
    return datetime.strptime(raw, "%Y-%m-%d").date()


def window_bounds(inc: dict, as_of: date) -> tuple[date, date, date]:
    """(đầu cửa sổ, cuối cửa sổ, mốc đọc input).

    lookback_days GỒM cả as_of: 3 ngày với as_of=14/08 là 12,13,14 — không phải 11..14.
    Mốc đọc input lùi thêm input_margin_days vì partition input có thể đo thời gian
    khác với ngày của output.
    """
    start = as_of - timedelta(days=inc["lookback_days"] - 1)
    return start, as_of, start - timedelta(days=inc.get("input_margin_days", 0))


def date_predicate(cols: list[str], lo: date, hi: date | None = None) -> str:
    """SQL lọc theo ba cột ngày. make_date để so sánh theo NGÀY — so từng cột rời
    (year >= .. AND month >= ..) là sai ở chỗ giao năm/tháng."""
    y, m, d = cols
    expr = f"make_date({y}, {m}, {d}) >= date'{lo.isoformat()}'"
    if hi is not None:
        expr += f" AND make_date({y}, {m}, {d}) <= date'{hi.isoformat()}'"
    return expr


def incremental_problems(plan: dict) -> list[str]:
    """Bất biến phải đúng TRƯỚC khi ghi. Vi phạm bất kỳ cái nào thì "ghi đè động"
    thoái hoá thành "ghi đè tất cả" — tức là xoá sạch lịch sử để thay bằng mỗi cửa
    sổ vừa tính. Đây là chốt chặn thảm hoạ, không phải kiểm tra hình thức."""
    inc, out = plan["incremental"], plan["output"]
    cols, part = inc["date_columns"], out.get("partition_by") or []
    problems = []
    if out.get("format", "parquet") != "parquet":
        problems.append(f"incremental chỉ hỗ trợ format=parquet, spec khai {out.get('format')!r}")
    if out.get("mode") != "overwrite":
        problems.append(f"incremental cần mode=overwrite (ghi đè ĐÚNG partition trong cửa sổ); "
                        f"mode={out.get('mode')!r} sẽ nhân bản dữ liệu khi Airflow retry")
    if not part:
        problems.append("incremental cần output.partition_by — không có partition thì "
                        "ghi đè động không có gì để khoanh vùng và sẽ ghi đè TẤT CẢ")
    elif part[:len(cols)] != cols:
        problems.append(f"date_columns {cols} phải là tiền tố của partition_by {part} — "
                        "lệch thì cửa sổ lọc theo cột này mà ghi đè lại khoanh theo cột kia")
    return problems


def main():
    with open(os.environ["JOB_PLAN"], encoding="utf-8") as f:
        plan = json.load(f)

    out = plan["output"]
    full_refresh = os.getenv("FULL_REFRESH", "").strip() not in ("", "0")
    inc = None if full_refresh else plan.get("incremental")

    is_iceberg = out.get("format") == "iceberg"
    spark = build_spark(plan["name"], iceberg=is_iceberg)
    spark.sparkContext.setLogLevel("WARN")

    windowed: set[str] = set()
    if inc:
        problems = incremental_problems(plan)
        if problems:
            raise SystemExit("SPEC SAI (incremental):\n  - " + "\n  - ".join(problems))
        start, end, read_from = window_bounds(inc, as_of_date())
        windowed = set(inc["windowed_inputs"])
        print(f"  cửa sổ  {start} .. {end}  (đọc input từ {read_from})")
    elif plan.get("incremental"):
        print("  FULL_REFRESH: bỏ qua cửa sổ, tính lại toàn bộ lịch sử")

    # Input: mỗi parquet -> view có tên (SQL tham chiếu tên này).
    for i in plan["inputs"]:
        df = spark.read.parquet(i["path"])
        note = ""
        if i["view"] in windowed:
            missing = [c for c in inc["date_columns"] if c not in df.columns]
            if missing:
                # Im lặng không lọc thì job vẫn chạy nhưng đọc toàn bộ — mất sạch ý
                # nghĩa của incremental mà không ai biết. Thà đỏ.
                raise SystemExit(f"input {i['view']} thiếu cột partition {missing}; "
                                 f"có: {sorted(df.columns)}")
            df = df.where(date_predicate(inc["date_columns"], read_from))
            note = f"  [cắt từ {read_from}]"
        df.createOrReplaceTempView(i["view"])
        print(f"  view {i['view']:<24} <- {i['path']}{note}")

    # Transform: SQL khai trong spec.
    result = spark.sql(plan["sql"])

    if inc:
        # Chỉ giữ partition NẰM TRỌN trong cửa sổ. Bất biến sống còn: ghi đè động
        # thay THẲNG cả partition, nên một partition chỉ tính được một phần sẽ xoá
        # mất phần còn lại. input_margin_days kéo dữ liệu thừa vào, dòng này đẩy ra.
        result = result.where(date_predicate(inc["date_columns"], start, end))

    result = result.cache()
    rows = result.count()

    if is_iceberg:
        # CTAS Iceberg: tạo namespace rồi ghi đè bảng. Iceberg tự quản snapshot/time-travel.
        table = out["table"]
        namespace = table.rsplit(".", 1)[0]
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        result.writeTo(table).using("iceberg").createOrReplace()
        target = table
    else:
        if inc:
            # Đây mới là thứ chặn thảm hoạ: ở chế độ static (mặc định của Spark),
            # overwrite XOÁ TOÀN BỘ path rồi mới ghi cửa sổ — mất sạch lịch sử ngoài
            # cửa sổ. Đặt ở đây (runtime) thì đè được cả --conf truyền từ ngoài.
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

            part = out["partition_by"]
            missing = [c for c in part if c not in result.columns]
            if missing:
                raise SystemExit(f"kết quả SQL thiếu cột partition {missing}; "
                                 f"có: {sorted(result.columns)}")
            # In ĐÚNG những partition sắp bị thay. Chạy lại cùng AS_OF phải cho ra
            # đúng danh sách này — đó là cách đọc log để tự kiểm tính idempotent.
            touched = sorted(tuple(r) for r in result.select(*part).distinct().collect())
            print(f"  thay {len(touched)} partition: "
                  + ", ".join("/".join(str(v) for v in t) for t in touched[:8])
                  + (" ..." if len(touched) > 8 else ""))

        writer = result.write.mode(out["mode"])
        if out.get("partition_by"):
            writer = writer.partitionBy(*out["partition_by"])
        writer.format(out.get("format", "parquet")).save(out["path"])
        target = out["path"]

    scope = f" [{start}..{end}]" if inc else " [toàn bộ]"
    print(f"WROTE {plan['name']}: {rows:,} rows -> {target}{scope}")
    spark.stop()


if __name__ == "__main__":
    main()
