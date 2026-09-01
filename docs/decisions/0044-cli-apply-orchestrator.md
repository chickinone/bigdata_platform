# ADR-0044: `cli apply` — đưa thứ tự triển khai từ trí nhớ vào code

- **Status:** Accepted
- **Date:** 2026-09-01
- **Deciders:** Phan Truong

## Bối cảnh

Sau [ADR-0043](0043-cold-rebuild-findings.md), hệ thống có đủ ba mảnh của một control plane:
desired state (`metadata/`), apply (5 deployer), verify (`cli verify`, chạy theo lịch mỗi giờ). Nhưng
ba mảnh **không nối thành vòng**: `cli` có 5 lệnh và **không lệnh nào chạm tới engine**. Muốn artifact
vào engine phải gọi tay 13 điểm vào độc lập, theo một thứ tự **chỉ sống trong runbook**.

Lần dựng lạnh 30/08 cho thấy đó không phải vấn đề lý thuyết: bước "nạp `clickhouse/init/*.sql`" không
nằm trong lệnh nào, chỉ nằm trong một dòng tài liệu. Bỏ sót nó thì ClickHouse rỗng, Flink submit
**thành công**, rồi im lặng không ghi được — và không service nào báo lỗi.

## Quyết định

Thêm `python -m dataplatform.cli apply`: soạn các deployer theo đúng thứ tự phụ thuộc, có **điều kiện
tiên quyết** giữa các bước, và chạy `cli verify` ở cuối.

Nguyên tắc: **soạn, không thay.** Mọi bước vẫn gọi riêng được
(`python -m dataplatform.deployers.<tên> apply`). Đây là chủ đích — lúc 3 giờ sáng khi một connector
chết, cần chạy đúng cái đó chứ không phải chạy lại cả chuỗi. Terraform có `-target`, Kubernetes có
`rollout restart` cho từng deployment; automation tốt là automation **cho phép đi vòng qua nó**.

| # | Bước | Điều kiện trước |
|---|---|---|
| 1 | `connectors apply` | — |
| 2 | `clickhouse_migrate baseline` rồi `apply` | — |
| 3 | `flink_metrics apply` | **`clickhouse_schema` phải ĐẠT** |
| 4 | `spark_batch apply` | — |
| — | `cli verify` (7 verifier) | luôn chạy cuối |

`clickhouse_migrate baseline` là lệnh **mới**, tách riêng khỏi `apply` vì file init chứa bảng
`ENGINE=Kafka` sẽ treo nếu broker chưa sống — `apply` (migration mệnh lệnh) phải chạy được cả khi Kafka
chết ([ADR-0032](0032-versioned-migration-clickhouse.md)). Nó tồn tại vì `docker-compose` **không
mount** `clickhouse/init/`, nên trên volume mới ClickHouse trống trơn.

## Phần đắt giá không phải thứ tự, mà là `needs`

Thứ tự thôi chỉ tránh chạy sai trình tự. `needs` chặn được cả trường hợp engine **"Up" nhưng chưa sẵn
sàng** — đúng kiểu thất bại đã xảy ra: Flink không cần bảng ClickHouse để *submit*, nó chỉ cần bảng để
*ghi*. Nên không có gate thì job xanh, dữ liệu không tới đâu, và triệu chứng chỉ lộ ra nhiều giờ sau ở
một dashboard trống.

`tests/test_apply_chain.py` khoá ràng buộc này lại: `needs` phải trỏ tới verifier có thật, và Flink
**phải** đợi `clickhouse_schema`. Gỡ gate đi thì test đỏ.

OpenMetadata **không** nằm trong chuỗi mặc định (`--with-openmetadata` mới bật): nó là phiên riêng, cần
dừng stack chính để nhường RAM và cần ES của OM sống.

## Lần chạy đầu đã tự chứng minh giá trị

Ngay lần `cli apply` đầu tiên, bước 4 dừng với `container ... is not running`. Truy ra:
**`bigdata-spark-master` và `bigdata-spark-worker` đã exit 255 từ 30 phút trước** (không phải OOM), mà
toàn bộ phần còn lại của stack vẫn trông bình thường.

Nguyên nhân gốc: bốn service **không khai `restart:`** nên mặc định là `no` — `spark-master`,
`spark-worker`, `iceberg-rest`, `trino`. Docker Desktop khởi động lại thì 15 service quay về, bốn cái
này nằm im. Đã sửa: `restart: unless-stopped` cho cả bốn.

Đáng ghi lại vì đây chính là luận điểm của ADR này, tự nó chứng minh: **stack mất 2/19 container suốt
30 phút và không có gì báo.** Thứ phát hiện ra không phải healthcheck (Spark không có), không phải CI
(thuần tĩnh) — mà là lần đầu tiên có một lệnh **đi hết cả chuỗi và bắt buộc từng bước phải đậu**.

## Hệ quả

- Dễ hơn: dựng lại từ số không là một lệnh; thứ tự được version, review, test như mọi đoạn code khác.
- Dễ hơn: `--dry-run` in ra chuỗi bước + điều kiện trước mà không đụng gì.
- Khó hơn: thêm một deployer mới giờ phải nhớ khai vào `APPLY_STEPS` — nhưng `test_apply_chain.py`
  bắt được nếu khai sai module hoặc sai tên verifier.
- **Có chủ đích KHÔNG làm:** `cli apply` không tự chạy khi merge. Ở quy mô này thứ còn thiếu là *thứ tự
  được encode*, không phải *trigger tự động*. Áp tự động lên engine thật khi merge cần thêm môi trường
  (dev/staging/prod), cổng phê duyệt cho thay đổi không hồi phục được, và rollback tự động — chưa cái
  nào tồn tại.

## Phương án đã cân nhắc

- **Một script shell chạy tuần tự** — loại: không có điều kiện tiên quyết, không test được, và lại là
  một bản sao thứ hai của thứ tự (runbook + script), đúng thứ sprawl cả dự án đang diệt.
- **Xoá các deployer lẻ, chỉ giữ `cli apply`** — loại: mất khả năng sửa đúng một chỗ khi sự cố.
- **Cho `cli apply` chạy trong CI khi merge** — loại: runner GitHub không với tới stack local
  (cùng lý do đã chặn việc đặt lịch `cli verify` vào Actions — ADR-0043).
