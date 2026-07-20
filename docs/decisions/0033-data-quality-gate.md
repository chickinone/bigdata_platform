# ADR-0033: Data quality gate — luật chạy trên dữ liệu thật (Pha 7)

- **Status:** Accepted — verifier chạy 66 check trên Postgres+ClickHouse thật; bắt vi phạm (negative test)
- **Date:** 2026-07-20
- **Deciders:** Phan Trường

## Bối cảnh

Contract mô tả schema, nhưng "dữ liệu có ĐÚNG không" thì chưa gate: `risk_score` phải 0–100, `kyc_status`
chỉ vài giá trị, cột `nullable:false` không được null, PK phải unique. Trước nay các ràng buộc này chỉ là
**comment** trong contract — không ai thực thi. Cần một gate chạy trên dữ liệu thật, fail thì chặn promote.

## Quyết định

`verifiers/quality.py` + `metadata/quality/*.yaml`. Hai nguồn luật:

- **TỰ SUY từ contract** (không khai lại): `not_null` cho mọi cột `nullable:false`, `unique` cho
  `primary_key`. Contract đã nói, quality thực thi.
- **TƯỜNG MINH** (`metadata/quality/<dataset>.yaml`, validate JSON Schema): `range`, `accepted_values` —
  thứ contract chỉ mô tả bằng chữ, nay thành luật CHẠY ĐƯỢC.

Runner route theo layer: `oltp` → Postgres (`schema.table`), `metric` → ClickHouse (`db.table`). Mỗi check
là một câu SQL **đếm vi phạm**; > 0 là fail. Nguồn không chạy → SKIP (không thể kiểm, không giả vờ pass).
Exit 1 nếu có vi phạm → chặn promote.

### Vì sao gate ở lớp verifier (runtime), không phải CI tĩnh

Kiểm chất lượng cần DỮ LIỆU THẬT, không suy được từ metadata tĩnh (khác `check`/`compat`). Nên nó là gate
lúc **promote/deploy** (nguồn phải sống), cùng họ với các verifier khác (`clickhouse_schema`, `avro_schema`).

## Kiểm chứng (đo thật)

- **66 check đạt, 0 vi phạm** trên Postgres (4 dataset OLTP, seed thật) + ClickHouse (metric): not_null,
  unique, range(risk_score), accepted_values(kyc_status).
- **Negative test**: siết `range` risk_score xuống 0–50 → bắt đúng **57 vi phạm**, exit 1.

## Hệ quả

**Dễ hơn:** ràng buộc dữ liệu thành luật chạy được, chặn rác trước khi lan hạ nguồn. not_null/unique tự
theo contract — thêm cột `nullable:false` = tự có luật.

**Khó hơn / phải chấp nhận:**
- Gate cần nguồn SỐNG (promote-time), không chạy trên runner CI tĩnh. Nguồn down → SKIP (không chặn).
- v1 có 4 loại luật (not_null/unique/accepted_values/range). `freshness` (dữ liệu có mới không) + custom SQL
  để tăng sau.
- Chỉ dataset có đích query được (oltp→PG, metric→CH). Stream/alert (ES) chưa gate — cần cơ chế khác.

## Phương án đã cân nhắc

- **Great Expectations / Soda.** Loại (giờ): nặng + thêm dependency lớn cho lab; SQL-đếm-vi-phạm đủ và
  đúng tinh thần stdlib-first. Cân nhắc lại nếu cần suite phong phú.
- **Chỉ dựa CHECK constraint ở Postgres.** Loại: không phủ ClickHouse/lake, và không tập trung ở metadata.
- **Khai lại not_null/unique trong file quality.** Loại: trùng contract — tự suy từ `nullable`/`primary_key`.
