# ADR-0026: Sinh lineage graph + data catalog từ metadata — Pha 6

- **Status:** Accepted
- **Date:** 2026-07-19
- **Deciders:** Phan Trường

## Bối cảnh

Pha 6 hứa trả lời ba câu: *"cột `amount` chảy tới đâu?"*, *"dataset nào chứa PII?"*, *"ai sở hữu?"*.
Metadata **đã chứa đủ** để trả lời (dataset contract có owner/tags/cột pii; pipeline spec có
`source_urn`→`sink_urn` + biểu thức SQL), nhưng chưa được **ghép lại và phơi ra**. Thông tin nằm rải
trong nhiều file — chính xác là thứ lineage cần gom.

## Quyết định

Generator `lineage.py` ghép **graph lineage + data catalog thuần từ metadata** (không cần chạy engine),
sinh 2 artifact:
- `lineage/graph.json` — máy đọc (feed DataHub/OpenMetadata sau).
- `lineage/LINEAGE.md` — người đọc: sơ đồ mermaid + catalog + "PII chảy tới đâu" + lineage cột.

### Ghép cạnh từ mọi nguồn metadata đã có

| Cạnh | Từ đâu |
|---|---|
| dataset → ES / S3-bronze / ClickHouse | `dataset.sinks` |
| source → sink (metric, fraud) | pipeline stream Flink (`source_urn`→`sink_urn`) |
| bronze → silver → gold/iceberg | pipeline batch Spark (input path → output); bronze topic ánh xạ về dataset urn |

### Lineage cột (Flink)

Mỗi cột metric có `expr`; regex `after.X` cho ra cột nguồn. Ví dụ:
`bank.metric.timeseries.total_amount` ← `bank.public.transactions.amount` (từ `SUM(CAST(after.amount...))`).

### Vì sao suy từ SPEC, không phải runtime

Có thể lấy lineage cột từ logical plan Spark lúc chạy. Nhưng suy từ spec **thuần metadata**: chạy được
không cần stack, đi vào `check` (đồng bộ tự động), và là "sự thật khai báo" chứ không phải quan sát runtime
có thể đổi. Column lineage cho Spark SQL (phức tạp hơn) để dành — cần parse SQL hoặc logical plan.

## Kiểm chứng

`check` **18/18** (thêm 2 artifact lineage). Graph bắt đúng dòng chảy chéo engine, và lôi ra một phát hiện
**governance thật**: PII của `customers` (`full_name/email/phone`) và `accounts` (`account_number`) **chảy
vào `silver:enriched_transactions`** (lake) — trước đây không ai thấy được điều này bằng mắt qua nhiều file.

## Hệ quả

**Dễ hơn:** ba câu hỏi Pha 6 trả lời bằng đọc một file, sinh lại tự động khi metadata đổi. `graph.json` là
nền để nạp vào DataHub/OpenMetadata (increment sau).

**Khó hơn / phải chấp nhận:**
- Lineage cột mới có cho **Flink metric**; Spark (SQL medallion) mới ở mức **dataset**, chưa cột — cần
  parse SQL/logical plan (sau).
- "PII chảy tới đâu" là xấp xỉ **mức dataset** (dataset chứa PII → mọi đích của nó), chưa truy chính xác
  cột PII nào rơi vào đích nào ở tầng lake.

## Việc còn lại Pha 6

- [ ] Lineage cột cho Spark (parse SQL) — chính xác hoá "PII chảy tới đâu" tới mức cột.
- [ ] Nạp `graph.json` vào **OpenMetadata/DataHub** để có UI + search (cần hạ tầng mới).
- [ ] Verify Trino federation runtime (ADR-0025).

## Phương án đã cân nhắc

- **Dựng DataHub/OpenMetadata ngay.** Bị hoãn: cần hạ tầng mới; `graph.json` là bước chuẩn bị để nạp vào.
- **Lineage cột từ logical plan Spark (runtime).** Bị hoãn cho Spark: ưu tiên suy-từ-spec (thuần metadata,
  vào `check`). Runtime plan chính xác hơn cho SQL phức tạp — làm khi cần độ chính xác cột ở lake.
