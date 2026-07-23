# ADR-0038: Governance trong OpenMetadata sinh từ metadata (domain, tier, test case, metric, dashboard, KPI)

- **Status:** Accepted
- **Date:** 2026-07-23
- **Deciders:** Phan Trường

## Bối cảnh

Catalog OM đã có table + PII tag + lineage cột ([ADR-0027](0027-openmetadata-catalog.md)), nhưng phần
governance của OM vẫn trống: không domain/tier, không owner thật (owner chỉ nằm trong description),
cột toàn kiểu STRING, tab Data Quality không có test case, Insights không có KPI, và Grafana — điểm
cuối thật sự của luồng metric — không xuất hiện trong lineage.

Muốn thêm các thứ đó có hai đường: gõ tay trên UI (nhanh, nhưng phá nguyên tắc một nguồn sự thật —
UI và Git sẽ lệch dần), hoặc mở rộng contract + deployer để mọi thứ sinh từ `metadata/` như phần
còn lại của platform.

## Quyết định

Đi đường thứ hai. Cụ thể:

1. **Contract thêm 2 trường** (`dataset.schema.json`): `domain` (core-banking / analytics / risk-fraud)
   và `tier` (chuẩn Tier1–Tier5 của OM). `graph.json` mang thêm domain/tier/description
   (`generators/lineage.py`).
2. **Connection registry thêm type `grafana`** với khối `dashboards` — bản kê dashboard (name/title/uid
   + danh sách URN dataset nguồn). Grafana giờ là một connection như mọi hệ thống khác.
3. **Deployer OM mở rộng** (`deployers/openmetadata.py`), chia hai mức nghiêm ngặt:
   - *Lõi* (service/schema/table/lineage): lỗi là dừng. Cột nay lấy kiểu + mô tả thật từ contract.
   - *Enrichment*: teams (từ `owner`), domains, classification `Sensitivity` (Confidential/Internal/
     Restricted theo layer), tier tag, **test case** OM, **metric entity**, **dashboard Grafana +
     cạnh lineage** ClickHouse→dashboard, **2 KPI** Insights (coverage description/ownership).
     Mỗi phần lỗi thì in `[CHÚ Ý]` rồi đi tiếp — API enrichment đổi theo version OM, không để một
     endpoint lệch giết cả lần nạp.
4. **Data quality theo đủ 4 lớp của mô hình OM**, không khai luật mới:
   | Lớp | Nguồn ở đây |
   |---|---|
   | TestDefinition | Built-in của OM, map từ `kind` của quality spec (`columnValuesToBeNotNull/Unique/Between/InSet`) |
   | TestCase | Sinh từ `case_specs()` của `verifiers/quality` — not_null/unique tự suy từ contract, range/accepted_values từ `metadata/quality/*.yaml` (66 case) |
   | TestSuite | Basic per-table OM tự tạo khi thêm case; **logical suite theo domain** (`core-banking-quality-suite` 40 case, `analytics-quality-suite` 26 case) do deployer gom |
   | TestCaseResult | `verifiers/quality --push-om` đẩy kết quả mỗi lần gate chạy thật (time-series: Success/Failed/Aborted + thông điệp) |

   `case_specs()` là nơi duy nhất định nghĩa "dataset này có check nào" — verifier dựng SQL từ nó,
   deployer dựng TestCase từ nó, tên case ổn định để FQN kết quả treo vào. Một luật, một nguồn,
   ba chỗ nhìn thấy (gate CLI, catalog, time-series).
5. **Idempotent bằng replace nguyên khối**: tags/domains/owners được PATCH thay cả mảng theo
   contract, không append — chạy lại không nhân đôi tag.

## Hệ quả

- Sửa domain/tier/owner/mô tả = sửa contract rồi `openmetadata apply` — UI không bao giờ là nơi sửa.
- Lineage giờ đi tới điểm cuối thật: Postgres → Kafka → ClickHouse → **Grafana dashboard**.
- Test case trong OM là *hiển thị* của quality gate, không phải hệ luật thứ hai; kết quả chạy thật
  vẫn do `verifiers/quality` đảm nhiệm và đẩy lên bằng `--push-om` (verify live: 66/66 Success,
  suite tổng hợp đúng theo domain).
- Trả giá: deployer phụ thuộc bề mặt API OM (1.12.6). Đã cô lập rủi ro bằng `_soft` — nâng version
  OM làm gãy enrichment nào thì thấy ngay `[CHÚ Ý]`, catalog lõi vẫn nạp đủ.

## Phương án đã cân nhắc

- **Gõ governance trên UI OM.** Loại: lệch Git-là-nguồn-sự-thật; không diff/review/rollback được.
- **Dùng ingestion connector Grafana của OM.** Loại: cần Grafana chạy cùng lúc (RAM không cho phép,
  xem [ADR-0027](0027-openmetadata-catalog.md)) và kéo cả dashboard rác; bản kê trong registry đủ
  cho lineage + discovery.
- **Khai luật quality riêng cho OM.** Loại: hai hệ luật sẽ lệch nhau — đúng loại sprawl đã diệt.
