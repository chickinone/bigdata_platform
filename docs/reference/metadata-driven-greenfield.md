# Triển khai Metadata-Driven cho dự án mới — yêu cầu & việc cần làm

> Sổ tay (playbook) cho một platform xây từ số 0 muốn metadata-driven ngay từ đầu. Đây là tài liệu
> **tham khảo tổng quát**, không gắn với hiện trạng `bigdata-platform` (dự án này là brownfield —
> mã hóa ngược hệ thống đã có, xem [`../roadmap/BDP-metadata-driven-roadmap.md`](../roadmap/BDP-metadata-driven-roadmap.md)).
> Cập nhật lần cuối: 2026-07-16.

---

## 0. Câu hỏi phải trả lời trước tiên: dự án của bạn có nên metadata-driven không?

Đây là phần quan trọng nhất, và là chỗ nhiều team bỏ qua rồi trả giá.

**Metadata-driven không miễn phí.** Bản chất nó là *xây một compiler cho hạ tầng dữ liệu*: registry +
generator + deployer + CI. Đó là một sản phẩm phần mềm phải bảo trì, không phải vài file YAML. Chi phí
thật nằm ở **người vận hành control plane**, không phải ở compute.

Vì vậy, nó chỉ đáng khi lợi ích vượt chi phí đó. Lợi ích tỉ lệ với:

> **số thực thể × số engine đích × tần suất thay đổi**

Khi tích số này **nhỏ**, viết tay config **rẻ hơn** — và xây control plane là over-engineering.

| Tình huống | Khuyến nghị |
|---|---|
| < 10 thực thể, 1–2 engine, ít đổi | **Đừng** xây control plane. Viết tay + review PR là đủ. |
| 10–50 thực thể, 3–5 engine, đổi thường xuyên | **Cân nhắc** metadata-driven cho tầng lặp nhiều nhất (thường là ingestion). |
| 50+ thực thể, nhiều engine, nhiều team | Metadata-driven **gần như bắt buộc** — sprawl sẽ giết bạn nếu không. |
| Team < 3 người, chưa có DE có kinh nghiệm | **Đừng.** Ưu tiên ship use case đầu tiên bằng công cụ managed. Xây platform sau. |

**Dấu hiệu bạn thực sự cần nó** (không phải "nghe hay"):
- Cùng một schema đang bị chép tay ở ≥ 3 công cụ và bạn đã từng bị lệch schema âm thầm.
- Thêm một cột/bảng buộc sửa nhiều file ở nhiều công cụ, và có người đã bỏ sót.
- Không trả lời tự động được "cột này chảy tới đâu / PII nằm ở đâu / ai sở hữu".

Nếu chưa gặp đau nào ở trên, **hoãn lại**. Xây control plane để phòng một nỗi đau chưa tồn tại là
resume-driven architecture.

> **Quy tắc vàng:** đừng xây platform trước khi ship được use case đầu tiên. Metadata-driven là cách
> *vận hành* platform, không phải cách *khởi động* nó.

---

## 1. Yêu cầu tiên quyết — bạn phải có gì trước khi bắt đầu

### 1.1 Thông tin phải thu thập (3 nhóm)

Greenfield khác brownfield ở chỗ: sự thật **chưa tồn tại**, phải moi ra từ nghiệp vụ + nguồn, không
có file cũ để chép.

| Nhóm | Thu thập gì | Lấy từ đâu |
|---|---|---|
| **① Nghiệp vụ & sở hữu (WHAT/WHO)** | Data domain, kiểm kê source system, danh sách thực thể + ý nghĩa, owner mỗi dataset, phân loại PII/compliance, SLA/freshness | Họp với business + data owner |
| **② Schema nguồn (HOW nguồn chạy)** | Schema (DDL/API spec/sample), primary key, update semantics (append-only vs mutable → CDC được không), volume/throughput, kiểu mã hóa đặc biệt | DBA/dev của hệ nguồn |
| **③ Nhu cầu tiêu thụ (WHERE/WHY)** | Ai consume, query kiểu gì, độ trễ chấp nhận được, cần search/OLAP/lake | Analyst/consumer |

> Thiếu nhóm ① là lỗi phổ biến nhất: team lao vào thiết kế schema kỹ thuật mà không biết **ai sở hữu**
> và **giữ bao lâu**. Owner và retention phải có **trước**, không phải bổ sung sau.

### 1.2 Năng lực đội ngũ

Metadata-driven đòi kỹ năng **software engineering**, không chỉ SQL:
- Ai đó phải bảo trì generator (code), CI, deployer. Đây là on-call cho control plane.
- Nếu team thuần analyst SQL, đây là rào cản thật — hoặc thuê DE, hoặc chọn công cụ đã metadata-driven
  sẵn (dbt cho transform) thay vì tự xây.

---

## 2. Hai nguyên tắc phải cam kết ngày-0

Đổi hai cái này về sau **cực đắt**. Chốt trước khi viết dòng code nào.

**① Tách control plane / data plane.** Không engine nào được giữ "sự thật". Connector không tự khai
danh sách bảng; job xử lý không tự khai schema. Mọi thứ **nhận** config từ ngoài.

**② Single source of truth.** Mỗi sự thật khai đúng một chỗ, sinh ra mọi nơi khác. Không có "bản sao
thứ hai được phép sửa tay".

---

## 3. Kiến trúc — các thành phần phải dựng

```
┌──────────────────────── CONTROL PLANE ───────────────────────┐
│  ① METADATA REGISTRY ──────────────────┐                      │
│     meta-schema + storage + validator   │                      │
│                       │                 ▼                      │
│  ② GENERATORS        ③ CI/CD        ④ CATALOG/LINEAGE          │
│     contract→artifact   validate→plan→apply                   │
│                       │                                        │
│                       ▼                                        │
│  ⑤ DEPLOYER (reconcile desired→actual, idempotent)            │
└───────────────────────┼──────────────────────────────────────┘
                        ▼
┌──────────────────────── DATA PLANE ──────────────────────────┐
│  ⑥ ENGINES phải CONFIG-DRIVEN (không hardcode từng pipeline)  │
└──────────────────────────────────────────────────────────────┘
```

| # | Thành phần | Trách nhiệm | Ngày-0 tối thiểu |
|---|---|---|---|
| ① | **Registry + meta-schema** | Nơi khai báo sự thật; định nghĩa "contract hợp lệ là gì" | ERD metadata + validator. **Bắt đầu YAML+Git**, đừng dựng DB vội |
| ② | **Generators** | Dịch `contract → artifact` (WHAT → HOW), mỗi engine một cái | Pure function, test được, diff được |
| ③ | **CI/CD** | `validate → plan → apply` + compatibility gate | Ít nhất chạy validate + diff ("plan") tự động |
| ④ | **Catalog/Lineage** | Nơi tra cứu, suy lineage từ pipeline spec | **Defer được**, nhưng thiết kế spec để lineage suy ra được |
| ⑤ | **Deployer** | Đưa artifact vào engine, idempotent + reconcile | Có thể làm tay lúc đầu; tự động hóa ở bậc sau |
| ⑥ | **Engine config-driven** | Thực thi theo config, không giữ sự thật | **Bắt buộc đúng ngay** — xem §5 |

---

## 4. Bốn quyết định "đắt-nếu-sai" — chốt đúng từ đầu

### 4.1 Hệ định danh (URN / addressing scheme)
Mọi thứ tham chiếu nhau qua URN, vd `domain.schema.table`. Đây là "primary key của cả platform". Đổi
scheme về sau = sửa mọi contract + mọi lineage. **Chốt đầu tiên.**

### 4.2 Mô hình môi trường & secret
- `dev/stg/prod` mô hình hóa thế nào trong metadata.
- Metadata **chỉ chứa `secret_ref`**, không bao giờ chứa giá trị. (Dùng SOPS+age hoặc Vault; thêm quét
  secret trong CI.)

### 4.3 Ranh giới control/data plane nằm ở đâu
Chốt rõ cái gì là "khai báo" (control) vs "chi tiết runtime" (data). Sai chỗ này thì hoặc metadata
phình ra ôm cả chi tiết engine, hoặc engine lại giữ sự thật → sprawl quay lại.

### 4.4 Chọn engine chịu được config từ ngoài
Xem §5 — đây là quyết định quyết định thành/bại.

---

## 5. Cái bẫy lớn nhất: engine phải config-driven, nếu không là "giả"

> **Registry đẹp mấy cũng vô dụng nếu data plane hardcode.**

Nếu mỗi pipeline là một đoạn code riêng (vd mỗi metric một file Python), thì dù có contract, bạn **vẫn
phải sửa code** mỗi lần thêm việc. Đó **không phải** metadata-driven — chỉ là "có thêm YAML bên cạnh".

Metadata-driven thật đòi **generic runtime**: một engine đọc spec → tự dựng công việc. Thêm metric =
thêm 1 file spec, **không viết code**.

Vì vậy **khi chọn công nghệ ngày-0, ưu tiên thứ nhận config ngoài được**:

| Ưu tiên (config-driven sẵn) | Tránh (buộc hardcode) |
|---|---|
| Flink **SQL** / Spark **SQL** | DataStream/RDD code rải rác từng pipeline |
| dbt (transform bằng SQL + YAML) | ETL script viết tay từng-cái-một |
| Kafka Connect (JSON config) | Consumer tự viết cho mỗi topic |
| Iceberg/Delta (bảng declarative) | Đường dẫn Parquet hardcode |

Đây cũng là phần **rủi ro cao nhất** khi xây (viết generic runtime khó hơn viết pipeline cụ thể). Nên
thường tổng quát hóa **tầng ingestion trước** (dễ, ROI cao), Flink/Spark runner sau.

---

## 6. Xây gì ngay, hoãn gì — bậc thang trưởng thành

Cạm bẫy ngược lại của over-engineering: dựng cả 6 thành phần + DB + catalog ngày đầu → tốn tháng chưa
ra data. **Đủ khung để không phải đập đi, nhưng chỉ xây khi cần.**

| Bậc | Xây ngay | Hoãn |
|---|---|---|
| **0 — MVP** | Meta-schema + YAML/Git + 1 generator + `check` (diff) | — |
| **1** | Thêm generators; CI chạy `check` | Deployer tự động |
| **2** | Deployer idempotent | Catalog |
| **3** | Compatibility gate + quality gate | Registry DB |
| **4** | Catalog/lineage, RBAC, drift detection | — |

**Phải đúng ngay bậc 0** (đắt khi sửa): URN scheme, meta-schema, ranh giới hai plane, chọn engine
config-driven. **Thêm dần được**: DB, catalog, deployer tự động, gate.

---

## 7. Lộ trình crawl → walk → run

Không big-bang. Mỗi pha neo vào một use case đã ship.

| Pha | Thời gian | Làm gì | Đội ngũ |
|---|---|---|---|
| **Crawl** | 0–3 tháng | 1 use case giá trị cao, đầu-cuối, trên stack mỏng nhất. Mã hóa **1 thực thể + 1 generator + check**. Chứng minh mô hình chạy. | 1–2 DE |
| **Walk** | 3–9 tháng | Nhân rộng generators; CI hóa `check`; deployer idempotent; catalog cơ bản; kiểm soát chi phí. Onboard 2–3 domain. | + platform owner |
| **Run** | 9–24 tháng | Generic runtime (Flink/Spark), quality gate, compatibility gate, lineage, self-serve. Federation nếu quy mô đòi. | platform team |

---

## 8. Anti-pattern — thấy là gọi tên

- **Xây control plane khi chưa ship use case nào.** Platform là cách *vận hành*, không phải cách *khởi động*.
- **Metadata-driven cho 5 bảng, team 3 người.** Over-engineering; viết tay rẻ hơn.
- **Registry đẹp nhưng engine hardcode.** Metadata-driven giả — vẫn phải sửa code mỗi lần đổi.
- **Sinh artifact mà không có bước diff/plan.** Generator sai → hỏng hàng loạt. `check` (diff rỗng
  trước khi apply) là bắt buộc.
- **Cho phép sửa tay artifact "cho nhanh".** Đây là drift — giết single source of truth. Nhãn "FILE
  Sinh tự động" + CI chặn là tối thiểu.
- **Dựng catalog/DB/mesh trước khi có basics** (registry + generator + check chạy được).
- **Bỏ qua owner & retention** ở nhóm thông tin ① vì "lo kỹ thuật trước".

---

## 9. Checklist ngày-0 (in ra, tick từng ô)

**Trước khi viết contract đầu tiên:**
- [ ] Đã xác định metadata-driven **đáng làm** (tích số thực thể×engine×tần-suất đủ lớn — §0)
- [ ] Đã thu thập đủ 3 nhóm thông tin (nghiệp vụ+owner / schema nguồn / tiêu thụ — §1.1)
- [ ] Đội có kỹ năng SE để bảo trì control plane, hoặc đã chọn công cụ metadata-driven sẵn (§1.2)
- [ ] Cam kết tách control/data plane + single source of truth (§2)

**Bốn quyết định đắt-nếu-sai (§4):**
- [ ] Chốt **URN scheme**
- [ ] Chốt mô hình **env + secret_ref** (không để giá trị secret trong metadata)
- [ ] Chốt **ranh giới** control/data plane
- [ ] **Chọn engine config-driven** (SQL/dbt/Connect/Iceberg — không hardcode) — §5

**MVP bậc 0 (§6):**
- [ ] Viết **meta-schema** (ERD + JSON Schema validator)
- [ ] Mã hóa **1 thực thể** làm mẫu
- [ ] Viết **1 generator** + lệnh **`check`** (diff), chứng minh sinh ra khớp mong đợi
- [ ] Có phép thử ngược: cố tình làm sai để chắc `check` biết báo đỏ

**Đã đúng những cái trên thì nhân rộng — đừng nhân rộng khi khung còn sai.**

---

## 10. Một câu chốt

Metadata-driven là **kỷ luật**, không phải công cụ. Thứ quyết định thành/bại không phải bạn dùng
DataHub hay OpenMetadata, mà là: (1) bạn có **thật sự đủ đau** để cần nó không, (2) engine của bạn có
**chịu được config từ ngoài** không, và (3) bạn có giữ được **single source of truth** không cho ai
sửa tay artifact. Sai một trong ba, mọi công cụ đắt tiền cũng vô nghĩa.
