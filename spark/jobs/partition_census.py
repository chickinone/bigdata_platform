import json
import sys

from medallion_runner import build_spark

# Đếm số dòng theo từng partition của một hay nhiều path. Dùng để chứng minh ba điều
# về đường incremental (ADR-0042): parity với full refresh, chạy lại không đổi, và
# partition NGOÀI cửa sổ không bị đụng. Nhận nhiều path trong một lần chạy vì mỗi lần
# spark-submit tốn cả phút khởi động JVM + tải package.

COLS = ["year", "month", "day"]


def census(spark, path):
    df = spark.read.parquet(path)
    present = [c for c in COLS if c in df.columns]
    out = {"total": df.count(), "partitions": {}}
    for r in df.groupBy(*present).count().orderBy(*present).collect() if present else []:
        # NULL là giá trị partition hợp lệ (__HIVE_DEFAULT_PARTITION__) — format %02d
        # trên None sẽ nổ TypeError, mà đó lại đúng thứ cần nhìn thấy chứ không phải giấu.
        key = "-".join("NULL" if r[c] is None else
                       (str(r[c]) if c == "year" else f"{int(r[c]):02d}")
                       for c in present)
        out["partitions"][key] = r["count"]
    return out


def main():
    spark = build_spark("partition_census", iceberg=False)
    spark.sparkContext.setLogLevel("ERROR")
    result = {}
    for path in sys.argv[1:]:
        try:
            result[path] = census(spark, path)
        except Exception as exc:                       # path chưa tồn tại là thông tin
            result[path] = {"error": type(exc).__name__}
    print("CENSUS " + json.dumps(result, ensure_ascii=False, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
