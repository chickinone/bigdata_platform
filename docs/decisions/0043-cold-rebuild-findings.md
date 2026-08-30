# ADR-0043: Lỗ hổng phát hiện lộ ra khi dựng lạnh — CRLF, init không tự chạy, quan sát thưa

- **Status:** Accepted — vá cả 4, verify live xong (gồm phép thử âm); đã đặt lịch và verify live cả chiều bắt lỗi
- **Date:** 2026-08-30
- **Deciders:** Phan Truong

## Bối cảnh

Toàn bộ stack bị `docker system prune -a` xoá sạch — container, volume **và** image. Việc phải dựng
lại từ số không hoá ra là một bài kiểm tra mà chạy hằng ngày không bao giờ cho: nó ép mọi bước khởi
tạo phải tự chứng minh, thay vì dựa vào trạng thái đã tồn tại từ trước.

Hai lỗi lộ ra. Điều đáng ghi lại không phải bản thân hai lỗi — mà là **vì sao không thứ gì báo động**,
vì hai cái bị bỏ sót theo hai cách hoàn toàn khác nhau và cần hai cách vá khác nhau.

## Vấn đề 1 — Trino `unhealthy` vĩnh viễn dù hoàn toàn khoẻ

`core.autocrlf=true` (Windows) + `.gitattributes` chỉ phủ `*.sh` và `*.sql`, **không phủ
`*.properties`** → cả 4 file config Trino mang CRLF. Script `/usr/lib/trino/bin/health-check` là bash:

```sh
port=$(get_property http-server.http.port "$config")   # -> "8080\r"
endpoint="$scheme://localhost:$port/v1/info"           # -> "http://localhost:8080\r/v1/info"
```

curl trả `(3) URL using bad/illegal format`. Trino **vẫn phục vụ query bình thường** vì Java tự trim
CRLF khi đọc `.properties` — chỉ script bash gãy.

Đây là kiểu hỏng tệ nhất: **tín hiệu sức khoẻ luôn đỏ trong khi hệ thống khoẻ**. Cùng lập luận đã dùng
cho gitleaks ở [ADR-0041](0041-harden-perimeter.md) — *một cổng luôn đỏ là một cổng bị bỏ qua*. Hệ quả
thật: `depends_on: service_healthy` trỏ vào Trino sẽ treo mãi, và nếu Trino chết thật thì không ai
phân biệt được.

**Vì sao vô hình:** `\r` đẩy con trỏ về đầu dòng, nên `echo "$port"` in ra `8080` trông y hệt đúng.
Chỉ `wc -c` (5 ký tự thay vì 4) mới lộ.

### Cách kiểm

```bash
docker inspect -f '{{range .State.Health.Log}}{{.Output}}{{end}}' bigdata-trino | tail -3
```

Thấy `curl: (3)` = URL méo, không phải Trino chết. Xác nhận Trino vẫn sống:

```bash
docker exec bigdata-trino trino --execute "SHOW CATALOGS"
```

Đếm CR trong file config (đây mới là phép đo đáng tin, đừng nhìn bằng mắt):

```bash
for f in trino/etc/config.properties trino/etc/catalog/*.properties; do echo "$f CR=$(tr -cd '\r' < "$f" | wc -c)"; done
```

## Vấn đề 2 — ClickHouse 0 bảng trên volume mới

`docker-compose.yml` **không mount** `clickhouse/init/`, nên baseline DDL không bao giờ tự chạy. Trên
volume mới, ClickHouse trống trơn: Flink metric sink không có bảng đích, `dlq-processor` không có chỗ
ghi. Không service nào báo lỗi — chúng chỉ im lặng không ghi được.

Mục này đã được ghi là còn hở ở [`BDP-current-state.md` §4.1 #1](../architecture/BDP-current-state.md);
lần dựng lạnh này chứng minh nó có thật chứ không phải lo xa.

### Cách kiểm

```bash
docker exec bigdata-clickhouse clickhouse-client -q "SELECT count(*) FROM system.tables WHERE database='metrics'"
```

Kỳ vọng ≥ 16 (4 metric × 3 đối tượng + dlq × 3 + `notification_events`). Ra 0 = baseline chưa nạp.

## Vì sao không thứ gì báo động — hai nguyên nhân KHÁC nhau

### (a) `cli check` mù về CRLF — lỗi trong chính công cụ kiểm

```python
raw = path.read_text(encoding="utf-8")     # cli.py::_compare
```

Text mode với `newline=None` bật **universal newlines**: Python dịch `\r\n` → `\n` *trước khi* trả về.
Nên `raw == generated` không bao giờ thấy khác biệt. Docstring của chính hàm đó khẳng định *"so nguyên
văn... byte-match là hợp lý và chặt hơn"* — **nó không phải byte-match**.

Chứng minh (thực nghiệm đã chạy 30/08): nhét CRLF vào `trino/etc/config.properties` — đủ để giết
healthcheck — rồi chạy `cli check`, kết quả vẫn `19/19 artifact khớp tuyệt đối`.

Đây là lỗ hổng nghiêm trọng nhất trong ba cái, vì nó khiến **cổng nói dối**: file trên đĩa khác file
sinh ra ở mức byte, mà cổng báo khớp tuyệt đối.

### (b) Trino không có bộ dò nào

Grep `trino` trong `dataplatform/*.py` chỉ ra 2 file: `cli.py` và `generators/trino_catalog.py` — cả
hai đều là **đầu ra**, không phải giám sát. Sáu verifier phủ postgres schema/publication, kafka topics,
avro, clickhouse schema, quality. **Trino không nằm trong danh sách.**

→ Vấn đề 1 lọt vì **không tồn tại bộ dò**. Chạy tay toàn bộ verifier mỗi ngày cũng vẫn lọt.

### (c) Vấn đề 2 thì ngược lại: bộ dò CÓ, nhưng không ai gọi

`verifiers/clickhouse_schema.py:51` đã đoán trước đúng tình huống:

```python
return [f"bảng {db}.{table} KHÔNG tồn tại trong ClickHouse (chưa apply DDL?)"]
```

Chạy nó ngay sau `compose up` là nó báo 15 bảng thiếu. Nhưng verifier cần engine sống, mà CI chạy trên
máy ảo sạch — và `metadata-check.yml` chỉ có `push` + `pull_request`, **không có `schedule:`**.

→ Vấn đề 2 lọt vì **bộ dò đúng việc nhưng không bao giờ được đánh thức**.

### (d) Nền hơn: quan sát hạ tầng rất thưa

Chỉ **4/19 container có healthcheck** (`elasticsearch`, `kafka`, `postgres`, `trino`) — và cái duy nhất
của Trino lại do image cung cấp, không phải ta viết. 15 container còn lại gồm ClickHouse, Kafka Connect,
Flink×3, Spark×2, MinIO, Schema Registry **không có tín hiệu sức khoẻ nào**. Docker báo `Up` chỉ nghĩa
là tiến trình chưa chết — nó không nói ClickHouse có bảng hay Flink có job.

## Quyết định

1. Thêm `*.properties text eol=lf` vào `.gitattributes` và khử CR khỏi 4 file. **Đã làm** — Trino
   `healthy` sau 50 giây; `cli check` vẫn 19/19. Ép ở tầng git là cách duy nhất chắc chắn vì 3/4 file
   là bản **sinh**, không sửa tay được (cùng lý do đã áp cho `*.sh`).
2. Ghi nhận ba lỗ hổng còn mở ở mục cuối, không vá vội trong cùng thay đổi này.

## Hệ quả

- Dễ hơn: healthcheck Trino nay là tín hiệu thật, dùng được cho `depends_on: service_healthy`.
- Khó hơn: chưa gì cả — nhưng ba lỗ hổng dưới vẫn để ngỏ, và ADR này là bản ghi để không quên.
- Tài liệu cập nhật kèm: `docs/guide/runbook.md` (thêm gotchas + trình tự dựng lạnh).

## Phương án đã cân nhắc

- **Sửa tay 4 file `.properties`** — loại: 3/4 là bản sinh, lần `cli write` sau sẽ ghi đè, và lần
  checkout sau `core.autocrlf` lại đổi ngược.
- **Đặt `core.autocrlf=false` cho máy** — loại: là cấu hình cục bộ của một máy, không đi theo repo nên
  người khác clone vẫn dính. `.gitattributes` đi cùng repo.
- **Bỏ healthcheck của Trino cho đỡ đỏ** — loại thẳng: đó là giấu triệu chứng, và làm mất luôn khả năng
  biết Trino chết thật.

## Đã vá cả ba (30/08)

### 1. `cli check` nay so byte thật

`_compare` đọc bằng `read_text(encoding="utf-8", newline="")` — tắt universal newlines. Kèm theo:
`_diff_text` phát hiện trường hợp **chỉ khác kết thúc dòng** và nói thẳng ra, thay vì in diff hai dòng
trông y hệt nhau khiến không ai hiểu nổi.

Chứng minh (đã chạy): nhét CRLF vào `trino/etc/catalog/postgres.properties` →

```
[KHÁC] trino/etc/catalog/postgres.properties
        CHI khac KET THUC DONG — noi dung giong het (4 ky tu CR tren dia, ban sinh dung LF).
        Sua: `git add --renormalize <file>` (da co quy tac trong .gitattributes).
```

exit 1. Hoàn tác → 19/19.

**Nhưng phát hiện một điều quan trọng khi thử:** file gây ra sự cố — `trino/etc/config.properties` —
**không nằm trong 19 artifact**. Nó là file *viết tay*, chỉ 3 file `catalog/*.properties` mới được sinh.
Nên dù `check` có chặt tới đâu cũng **không bao giờ phủ được nó**. Thứ duy nhất bảo vệ nó là
`.gitattributes`. Đây là lý do việc dưới đây mới thật sự đóng lỗ hổng, chứ không phải việc #1.

### 2. `.gitattributes` phủ toàn repo

```
* text=auto eol=lf          # thu dong cua ca lop loi
*.cmd/.bat/.ps1 eol=crlf    # ngoai le duy nhat: script Windows
```

Ba dòng cũ (`*.sh`, `*.sql`, `*.properties`) giữ lại vì chúng ghi rõ ý định cho từng loại, nhưng dòng
`*` mới là thứ đóng lớp lỗi: file **sinh** như `.py` (DAG Airflow), `.json` (bản kê topic), `.md`
(lineage) trước đây không được phủ, nên `core.autocrlf=true` biến chúng thành CRLF ở mọi lần clone.

### 3. Verifier Trino + `cli verify`

`verifiers/trino_catalog.py` — đối chiếu catalog khai trong connection registry với Trino đang chạy.
Ba tầng kiểm, cố ý tăng dần độ chặt:

1. Catalog có tồn tại không (`SHOW CATALOGS`).
2. **Có query được không** (`SHOW SCHEMAS FROM <catalog>`) — tồn tại chưa đủ: file `.properties` hỏng
   vẫn tạo ra catalog nhưng query sẽ nổ.
3. Healthcheck container có `unhealthy` không — bắt đúng triệu chứng CRLF, và kiểm ở đây vì
   `config.properties` là file viết tay nên **không cổng nào khác nhìn thấy**.

`python -m dataplatform.cli verify` chạy cả 7 verifier, gộp thành một mã thoát. Điểm thiết kế đáng
lưu: nó **phân biệt hai loại thất bại**, vì chúng đòi hai phản ứng khác hẳn nhau.

| Mã | Nghĩa | Phải làm gì |
|---|---|---|
| 0 | mọi verifier đạt | — |
| 1 | engine sống nhưng **lệch** contract | sửa |
| 3 | **không tới được** engine | bật stack, chưa kết luận được gì |

Gộp hai loại này làm một sẽ biến "stack chưa bật" thành báo động giả — mà báo động giả lặp lại thì cả
cổng bị bỏ qua, đúng lập luận đã dùng cho `--no-git` của gitleaks ([ADR-0041](0041-harden-perimeter.md)).

Kèm `tests/test_verify_runner.py`: khoá `VERIFIERS` phải khớp đúng thư mục `verifiers/`. Thêm verifier
mới mà quên đăng ký thì test đỏ — nếu không, ta lại có thêm một "bộ dò tồn tại nhưng không ai gọi".

## Hai điều còn lại, nói cho sòng phẳng

**Verifier Trino — đã verify live (30/08).** Cả đường thành công lẫn đường thất bại:

| Tình huống | `SHOW CATALOGS` | `SHOW SCHEMAS FROM iceberg` | Verifier |
|---|---|---|---|
| Bình thường | có `iceberg` | chạy | `0 lech`, exit 0 |
| **Tắt `iceberg-rest`** | **vẫn có `iceberg`** | nổ | `[LOI] iceberg`, exit 1 |

Cột giữa là điểm mấu chốt: catalog **vẫn tồn tại** khi backend chết, nên một verifier chỉ kiểm
`SHOW CATALOGS` sẽ báo xanh. Tầng 2 (query thật) mới bắt được. Đây cũng chính là bẫy đã ghi từ lâu —
`tabulario/iceberg-rest` giữ catalog trong RAM — nay lần đầu có thứ tự động phát hiện.

Cũng xác nhận vá CRLF bền: sau khi Docker Desktop tắt đột ngột rồi bật lại, Trino `healthy` sau 50s.

**Đã đặt lịch (30/08) — vòng lặp khép kín.** `scripts/verify-scheduled.cmd` + Windows Task Scheduler,
chạy mỗi giờ.

Vì sao **không** dùng GitHub Actions: runner của GitHub là máy ảo sạch trên mạng khác, không với tới
được stack Docker ở máy local. `schedule:` trong `metadata-check.yml` là vô nghĩa cho verifier — lịch
bắt buộc phải chạy ở nơi tới được engine.

Vì sao chia đôi **script trong repo / lịch ngoài repo**: logic (chạy gì, phân loại ra sao, ghi log ở
đâu) phải được version và review như mọi thứ khác; riêng việc đăng ký là cấu hình của *từng máy*. Đổi
cách kiểm = sửa file trong repo rồi commit, không phải gỡ task ra đăng ký lại.

Script phân loại theo đúng ba mã thoát của `cli verify`, và **chỉ mã 1 mới trả lỗi cho Task Scheduler**:
`3` (stack chưa bật) trả 0, vì một cảnh báo đỏ mỗi giờ chỉ vì máy chưa bật Docker sẽ nhanh chóng bị bỏ
qua — cùng lập luận đã dùng cho `--no-git` của gitleaks ([ADR-0041](0041-harden-perimeter.md)).

Verify live, để chính scheduler gọi (không phải chạy tay):

```text
20:26  DAT   exit=0
20:27  DAT   exit=0   <- scheduler
20:28  LECH  exit=1   <- scheduler, sau khi tat iceberg-rest
20:29  DAT   exit=0   <- scheduler, sau khi bat lai
```

Đây là lần đầu hệ thống **tự phát hiện** một sự cố runtime mà không cần ai nhớ chạy gì. Bản ghi đáng
tin là `.verify/history.log` (đã gitignore), không phải cột "Last Result" của Task Scheduler.

Gỡ hoặc đổi tần suất:

```bash
schtasks /Delete /TN "BDP-verify" /F
schtasks /Change /TN "BDP-verify" /RI 360      # doi sang moi 6 tieng
```

**Ghi nhận thêm:** `verifiers/quality` trả **exit 0** khi nguồn không chạy (`0 đạt, 0 vi phạm, 66 bỏ
qua`). Đứng một mình đó là **xanh giả** — nó báo đạt trong khi không kiểm được gì. Trong `cli verify`
thì vô hại vì tổng hợp vẫn ra exit 3, nhưng nếu ai gọi `quality` riêng lẻ thì cần biết.
