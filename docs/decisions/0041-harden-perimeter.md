# ADR-0041: Siết vành đai — bỏ credential khỏi code, gate secret, validate pipeline spec, lưới test

- **Status:** Accepted — 33 test xanh, `check` 19/19, gitleaks sạch, quality gate 40 check live không cần credential
- **Date:** 2026-07-30
- **Deciders:** Phan Trường

## Bối cảnh

Rà soát toàn dự án sau khi lộ trình metadata-driven đóng ([ADR-0037](0037-cutover-complete-single-source.md)).
Kết quả đáng chú ý: **phần metadata-driven không có lỗ nào** — không tìm thấy chỗ nào một sự thật còn bị khai
hai nơi. Chỗ yếu nằm ở **vành đai**: bảo mật, lớp validate, và lưới test.

Năm điểm cụ thể:

1. **Credential thật trong code đã push.** `verifiers/quality.py` hardcode mật khẩu Postgres làm default của
   `os.getenv` — và giá trị đó trùng đúng `.env`. Nghịch lý: `.env` chưa bao giờ bị commit
   ([ADR-0013](0013-secrets-in-gitignored-env.md) giữ đúng), nhưng mật khẩu vẫn lọt ra bằng cửa khác.
2. **CI không quét secret.** Roadmap Pha 0 mục 2 đã ghi việc này, chưa làm — nên (1) không có gì chặn.
3. **Pipeline spec không được validate.** `datasets/`, `connections/`, `quality/` đều có JSON Schema;
   `pipelines/` thì không. Tệ hơn: spec bị `yaml.safe_load` ở **ba nơi độc lập** (`flink_sql`,
   `spark_batch`, `cli`), không nơi nào kiểm trùng tên. Spec thiếu trường → `KeyError` sâu trong generator.
4. **Định danh không có pattern.** `sinks.clickhouse.{database,table}`, `columns[].name`,
   `source.{schema_name,table}` chỉ có `minLength: 1`, mà chúng đi **thẳng** vào `f"CREATE TABLE {db}.{table}"`
   và Flink DDL. `urn` thì đã có pattern — nên đây là thiếu nhất quán chứ không phải quyết định.
5. **Không có test tự động nào.** ~4700 dòng control plane, 0 file test. `cli check` chỉ bắt **drift**,
   không bắt **logic sai**.

## Quyết định

### 1. Verifier không được cầm credential

`quality.py` dùng lại `_psql` của `verifiers/postgres_schema.py` — chạy psql **bên trong container** bằng
chính env `POSTGRES_USER`/`POSTGRES_DB` của nó. Credential trở thành **không cần thiết**, không phải "được
giấu kỹ hơn". Bonus: xoá luôn bản copy thứ hai của cách gọi psql.

> **Việc phải làm ngoài repo:** mật khẩu đã nằm trong git history. Xoá code không đủ — **phải đổi mật khẩu
> Postgres**. Đây là hành động trên hạ tầng, không phải thay đổi mã.

`deployers/openmetadata.py` giữ default công khai của OM (`admin`/base64) vì nó là default tài liệu hoá của
sản phẩm, nhưng ghi rõ: đổi credential = đặt env, không sửa dòng đó.

### 2. Gate gitleaks quét **working tree**, không quét history

```
gitleaks detect --source /repo --no-git --redact --exit-code 1
```

`--no-git` là quyết định có chủ ý: history chứa credential cũ đã xử lý bằng cách đổi mật khẩu; quét history
sẽ làm CI **đỏ vĩnh viễn** cho tới khi rewrite history — và một gate luôn đỏ là một gate bị bỏ qua. Mục tiêu
của gate là chặn secret **mới**, đúng thứ đã lọt một lần. `--redact` để không in giá trị ra log CI.

Chạy bằng docker image chính thức thay vì GitHub Action: không phụ thuộc điều kiện license của action, và
chạy được y hệt ở local.

### 3. Pipeline spec là contract hạng nhất

`schemas/pipeline.schema.json` + `registry.load_pipelines()`. Schema rẽ nhánh theo `engine` bằng `if/then`:

| engine | bắt buộc |
|---|---|
| `flink_sql` | `source_urn`, `sink_urn`, `window`, `aggregations` |
| `flink_datastream` | + `startup`, `source_columns`, `detectors` |
| `spark_sql` | `layer`, `inputs`, `sql`, `output` |

`additionalProperties: false` để typo (`filtr:` thay vì `filter:`) bị bắt thay vì im lặng bị bỏ qua — đây là
lớp lỗi nguy hiểm nhất của YAML.

**Ba loader gộp về một.** `flink_sql`, `spark_batch`, `cli` nay cùng gọi `registry.load_pipelines()` — cùng
lý do `postgres_publication` gọi `debezium.cdc_datasets()`: chỉ được có **một** nơi biết "spec hợp lệ là gì".
Kèm kiểm trùng `name` (hai spec cùng tên = ghi đè job plan/task Airflow của nhau).

### 4. Pattern định danh cho thứ đi vào DDL

`^[a-z_][a-z0-9_]*$` cho `source.{schema_name,table}`, `columns[].name`,
`sinks.clickhouse.{database,table}`, và các định danh tương ứng trong pipeline schema.

Đây **không phải** chống injection từ người dùng cuối — nguồn là repo, có CODEOWNERS và review. Nó là
defence-in-depth: một PR (vô ý hoặc cố ý) đặt `table: "x; DROP DATABASE metrics"` hiện sinh ra DDL **chạy
được**. Chi phí 5 dòng schema, nên không có lý do không làm.

### 5. `tests/` cho hàm thuần, chạy trong CI

33 test, không cần engine nào:

| File | Phủ gì |
|---|---|
| `test_contract_guards.py` | `_check_unique_urns/_topics`, metadata thật hợp lệ, **ràng buộc chéo file** (`source_urn`/`sink_urn` phải trỏ URN có thật — schema không kiểm được) |
| `test_compat_gate.py` | luật BACKWARD hai chiều, `encoded_as` thắng kiểu logic, thiếu `nullable` = optional |
| `test_flink_sql.py` | `_referenced_columns` (loại cột chết), source/sink type, `_assert_columns_match` gồm **sai thứ tự** |
| `test_kafka_topics_verifier.py` | `parse_describe`, whitelist broker-managed, chỉ so khoá bản kê khai |

Chọn hàm thuần là có chủ ý: chúng chạy trong 0.3s, không cần Docker, nên vào được **CI tĩnh** cùng `check`
và `compat`. Verifier cần stack vẫn thuộc nhóm promote-time như trước.

## Kiểm chứng (đo thật)

- **33 test xanh** trong 0.33s; `compileall` sạch.
- **`cli check` 19/19** sau toàn bộ thay đổi — refactor không đổi một byte artifact nào.
- **gitleaks:** quét 101 MB working tree → `no leaks found`, exit 0.
- **Quality gate live, không credential:** Postgres thật → **40 check đạt, 0 vi phạm**; đường parse giá trị
  khác 0 cũng kiểm (`count(*) = 26712`, `risk_score > 50 = 57`).
- **Test âm pipeline schema:** thiếu `sink_urn` → báo đúng file + `'sink_urn' is a required property`; typo
  `filtr` → `Additional properties are not allowed`; trùng tên → chỉ ra cả hai file.
- **Test âm pattern:** `table: "kpi; DROP DATABASE metrics"` → `ContractError ... does not match`.
- **Repo hygiene:** không volume nào bị commit; 50 MB tracked, gần hết là JAR Flink.

## Hệ quả

**Dễ hơn:** secret mới không lọt được. Pipeline spec sai bị bắt ở đúng file đúng dòng thay vì `KeyError`.
Sửa hàm thuần mà làm hỏng logic thì CI đỏ — trước đây không có gì bắt.

**Khó hơn / phải chấp nhận:**

- CI cần Docker cho bước gitleaks (runner GitHub có sẵn), và thêm ~15s.
- `--no-git` nghĩa là secret **đã có trong history vẫn nằm đó**. Xử lý bằng rotation, không bằng gate.
- `load_pipelines()` giờ validate mọi spec kể cả khi caller chỉ cần một engine — chậm không đáng kể, đổi lại
  một spec batch hỏng cũng làm `flink_metrics` đỏ. Đó là chủ ý: metadata sai là metadata sai.
- 50 MB JAR trong repo vẫn còn — nợ hygiene, chưa xử (gỡ khỏi history là thao tác xâm lấn).

## Phương án đã cân nhắc

- **Xoá mật khẩu khỏi git history (`filter-repo`/BFG).** Loại (giờ): rewrite history phá mọi clone/fork và
  không xoá được bản đã ai đó pull. Rotation giải quyết triệt để hơn với chi phí thấp hơn.
- **Quét history trong CI.** Loại: xem §2 — gate luôn đỏ là gate chết.
- **`gitleaks-action` thay vì docker image.** Loại: điều kiện license cho tổ chức, và không chạy lại được
  y hệt ở local.
- **Một schema riêng cho từng engine pipeline.** Loại: ba file schema cho một khái niệm, và `cli` sẽ phải
  biết chọn schema nào — đúng thứ sprawl đang diệt. `if/then` giữ một nguồn.
- **Test tích hợp có stack ephemeral trong CI.** Hoãn: đắt và chậm; hàm thuần cho ROI cao nhất trước. Verifier
  cần stack vẫn chạy promote-time như [ADR-0033](0033-data-quality-gate.md)/[0039](0039-verify-publication-vs-contract.md).
