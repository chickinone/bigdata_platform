# Kibana — điều tra fraud & failed transaction

> Data view, saved search, và dashboard điều tra. Kibana phục vụ **tra cứu chi tiết từng bản ghi** —
> khác Grafana vốn để theo dõi metric realtime từ ClickHouse.
> Dữ liệu do 5 ES sink đẩy sang: [`cdc-and-connectors.md`](cdc-and-connectors.md).
> Cập nhật lần cuối: 2026-07-15.

---

## 1. Năm index

http://localhost:5601 · Kiểm tra index đã có dữ liệu:

```bash
curl.exe http://localhost:9200/_cat/indices?v
```

| Index | Khóa document | Dùng để |
|---|---|---|
| `bankdb.public.customers` | `customer_id` | Tra cứu khách hàng, KYC, risk score |
| `bankdb.public.accounts` | `account_id` | Tra cứu tài khoản, balance, status |
| `bankdb.public.transactions` | `transaction_id` | Phân tích giao dịch & giao dịch lỗi |
| `bankdb.public.transfers` | `transfer_id` | Theo dõi lifecycle chuyển tiền |
| `fraud-alerts` | — | Điều tra cảnh báo gian lận |

---

## 2. ⚠️ Hai giới hạn về mapping cần biết trước

Cả hai bắt nguồn từ `schema.ignore=true` ở ES sink — ES **tự đoán** mapping thay vì lấy từ Avro schema.

**(a) Cột số bị map thành chuỗi.** Vì `decimal.handling.mode=string` của Debezium, `amount`/`balance`
đến ES dưới dạng chuỗi. ES map chúng thành `text`/`keyword`, nên **filter dạng số không hoạt động**:

```text
balance > 10000        ← KHÔNG chạy như mong đợi
status: "frozen"       ← chạy tốt, dùng cái này
```

**(b) Trường thời gian của fraud-alert không phải kiểu date.** Alert của Flink dùng epoch millis dạng
số (`window_start_ms`, `detected_at_ms`), nên Kibana không nhận là date → data view `fraud-alerts`
thường **không chọn được time field**.

Muốn sửa tận gốc: khai báo **index template** trong ES **trước khi** sink chạy lần đầu, ép kiểu cho
các trường này. Mapping không đổi được sau khi index đã tạo — phải reindex.

---

## 3. Tạo data view

```text
Stack Management → Data Views → Create data view
```

| Data view | Time field khuyến nghị |
|---|---|
| `bankdb.public.customers` | `updated_at` hoặc `created_at` |
| `bankdb.public.accounts` | `updated_at` hoặc `opened_at` |
| `bankdb.public.transactions` | `created_at` |
| `bankdb.public.transfers` | `updated_at` hoặc `initiated_at` |
| `fraud-alerts` | **Không chọn time field** (xem §2b) |

---

## 4. Saved search để điều tra

Dùng **Discover saved search** thay vì Lens cho các bảng tra cứu — mục tiêu là xem document chi tiết
và lọc theo field, không phải vẽ biểu đồ.

**Giao dịch lỗi gần nhất**
```text
Data view: bankdb.public.transactions
KQL:       status.keyword : "failed"
Columns:   transaction_id, account_id, transaction_type, amount, currency,
           merchant_category, description, created_at, status
Sort:      created_at descending
Save as:   Latest Failed Transactions
```

**Khách hàng rủi ro cao**
```text
Data view: bankdb.public.customers
KQL:       risk_score >= 80
Columns:   customer_id, full_name, email, country_code, kyc_status, risk_score, updated_at
Save as:   High-risk Customers
```

**Tài khoản bất thường**
```text
Data view: bankdb.public.accounts
KQL:       status.keyword: "frozen" OR status.keyword: "suspended"
Columns:   account_id, customer_id, account_number, account_type, currency, balance, status, updated_at
Save as:   Suspicious Accounts
```

**Điều tra fraud alert** (dạng bảng, vì thiếu time field)
```text
Data view: fraud-alerts
Chart:     Data table
Rows:      alert_type.keyword, severity.keyword, account_id
Metric:    Count
Save as:   Fraud Alerts Investigation
```

Field nên hiển thị cho từng view:

| View | Field |
|---|---|
| Customers | `customer_id`, `full_name`, `email`, `country_code`, `kyc_status`, `risk_score`, `updated_at` |
| Accounts | `account_id`, `customer_id`, `account_number`, `account_type`, `currency`, `balance`, `status`, `updated_at` |
| Transactions | `transaction_id`, `account_id`, `transaction_type`, `amount`, `currency`, `status`, `merchant_category`, `created_at` |
| Transfers | `transfer_id`, `from_account_id`, `to_account_id`, `amount`, `currency`, `status`, `failure_reason`, `updated_at` |

---

## 5. Biểu đồ (Lens)

**Phân bố loại alert**
```text
Data view: fraud-alerts
Chart:     Donut
Slice by:  alert_type.keyword     → VELOCITY_FRAUD, FAILED_STORM
Metric:    Count
```

**Giao dịch lỗi theo thời gian**
```text
Data view: bankdb.public.transactions
Chart:     Bar vertical
Filter:    status.keyword : "failed"
X-axis:    date histogram theo created_at
Y-axis:    Count of records
Save as:   Failed Transactions Over Time
```

---

## 6. Layout dashboard đề xuất

`Dashboard → Create dashboard → Add from library`

| Hàng | Panel |
|---|---|
| 1 | Alert Type Distribution · Failed Transactions Over Time |
| 2 | Fraud Alerts Investigation · Latest Failed Transactions |
| 3 | High-risk Customers · Suspicious Accounts |
| 4 | Transfers Search · Customers/Accounts Search |

Thiết kế theo **luồng điều tra** — mỗi hàng trả lời câu hỏi tiếp theo của hàng trên:

```text
Fraud alert → account_id → giao dịch lỗi → chi tiết account/customer → lifecycle chuyển tiền
```

---

## 7. Vấn đề thường gặp

| Triệu chứng | Nguyên nhân | Xử lý |
|---|---|---|
| Không có index | ES sink chưa đăng ký hoặc `FAILED` | `curl.exe http://localhost:8083/connectors?expand=status` |
| Index có nhưng rỗng | Chưa có dữ liệu CDC | Chạy generator |
| Filter `balance > 10000` không ra gì | `balance` bị map thành text (§2a) | Lọc theo `status` thay thế |
| `fraud-alerts` không chọn được time field | Trường `*_ms` là số, không phải date (§2b) | Dùng Data table, không dùng date histogram |
| Document nhân bản thay vì cập nhật | `extractKey.field` sai trong config sink | Kiểm tra field khớp PK của bảng |
| Kibana báo không kết nối được ES | ES chưa healthy | `docker logs bigdata-elasticsearch` |
