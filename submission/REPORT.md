# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Phạm Thanh Sơn - 02794
**Repo:** K4-Track02-Day17-Data-Pipeline-Engineering
**Commit bài nộp:** 12a15843477e774bf79efc3cc80dc3c178fca641
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity AI (Pair programming đồng hành: cùng phân tích log errors, thảo luận nguyên nhân gốc rễ, rà soát logic MERGE SQL / CDC staging và hỗ trợ kiểm thử pipeline).
**Nguồn tham khảo khác (nếu có):** Slide bài giảng Day 17 Data Pipeline Engineering.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 hàng cho 12 tickets; T-91 giữ lại 3 bản ghi lịch sử [('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')] thay vì chỉ 1 bản ghi mới nhất. | `gold_feature_daily` của u05 ngày 2026-08-12 chỉ đếm (2, 0) thay vì (5, 1) do event đến muộn ngày 08-15 bị bỏ qua khi `LOOKBACK_DAYS = 0`. | T-97 có lệnh xoá (CDC op='d') nhưng `silver_tickets` vẫn lưu `is_deleted=False` và giữ nguyên PII; T-97 vẫn xuất hiện ở training set và RAG index. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT INTO` append dữ liệu mà không deduplicate/MERGE theo `ticket_id`, và không kiểm tra `_lsn` để ngăn ghi đè dữ liệu cũ. | `LOOKBACK_DAYS` trong `config.py` đặt bằng 0, không bao phủ độ trễ của sự kiện (P99 lateness = 3 ngày), khiến các batch cũ không được recompute lại khi data muộn đến. | `ticket_changes_sql` chỉ lấy `ticket_id` từ `j->'value'->'after'`, nhưng sự kiện delete (`op='d'`) có `after = null`, dẫn tới bản ghi xoá bị loại bỏ hoàn toàn khỏi staging. |
| **Cách sửa** (file, vài dòng) | [pipeline/silver.py](file:///d:/Vin%20AI/Lab_Day17_Vin%20AI/K4-Track02-Day17-Pham-Thanh-Son-02794Data-Pipeline-Engineering/pipeline/silver.py): dùng `MERGE INTO silver_tickets ... WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. | [pipeline/config.py](file:///d:/Vin%20AI/Lab_Day17_Vin%20AI/K4-Track02-Day17-Pham-Thanh-Son-02794Data-Pipeline-Engineering/pipeline/config.py): đổi `LOOKBACK_DAYS = 3` để tính toán lại các partition trong cửa sổ `[day - 3, day]`. | [pipeline/staging.py](file:///d:/Vin%20AI/Lab_Day17_Vin%20AI/K4-Track02-Day17-Pham-Thanh-Son-02794Data-Pipeline-Engineering/pipeline/staging.py): dùng `COALESCE(after.ticket_id, before.ticket_id, key.ticket_id)` để bắt bản ghi `op='d'`, set `is_deleted=True` và xoá PII. |
| **Khái niệm trên slide** | Upsert / MERGE, Single Source of Truth, Out-of-order execution, Idempotency. | Event time vs Ingest time, Watermark / Lookback window, Partition Overwrite, Late-arriving data. | Change Data Capture (CDC), Debezium Schema, Tombstone, Hard delete vs Soft delete / GDPR compliance. |

## 2. Các con số

- P50 / P95 / P99 lateness đo từ Bronze: `0.00` / `2.90` / `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE giúp duy trì bảng thực thể duy nhất SCD1 theo khoá chính khi ghi theo batch, còn overwrite-partition cho phép recompute tính toán song song/idempotent theo cửa sổ ngày mà không phải xoá sửa từng dòng lẻ.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giúp ghi nhận trạng thái đã xoá (`is_deleted=True`) kèm LSN để ngăn các batch replay cũ vô tình khôi phục (resurrect) lại dữ liệu đã bị xoá.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến (immutability) và khả năng tái lập (reproducibility) của các phiên bản dataset huấn luyện mô hình ML mà không bị ảnh hưởng bởi cập nhật/xoá dữ liệu trong tương lai.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu cỡ nhỏ đến trung bình (GBs) chạy trực tiếp trên file/in-memory với DuckDB cho tốc độ cao, độ trễ cực thấp và không tốn chi phí quản lý cluster như Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   *Trả lời:* Đây là mâu thuẫn điển hình giữa tính toàn vẹn kiểm thử ML (Reproducibility) và tuân thủ pháp lý (GDPR "Right to be forgotten"). Trong thực tế production, tuân thủ pháp lý là bắt buộc vượt lên trên tính bất biến: khi có yêu cầu GDPR/Delete request, hệ thống cần thực hiện *Targeted Crypto-Shredding* (mã hoá PII từng user với key riêng và xoá key) hoặc *Snapshot Vacuuming / Re-snapshotting* (chạy lại script sinh snapshot lịch sử từ Bronze đã được làm sạch PII/Tombstone) để vừa đảm bảo không còn PII vừa loại bỏ dữ liệu bị xoá khỏi toàn bộ các snapshot quá khứ.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   *Trả lời:* Regex chỉ che được các chuỗi PII định dạng cố định (email, phone, căn cước). Để bắt được Tên riêng hay Địa chỉ (Unstructured PII), cần bổ sung chốt kiểm soát PII bằng mô hình NER (Named Entity Recognition) hoặc Presidio PII Scanner ngay từ tầng **Staging / Silver Ingestion**. Đo lường chất lượng chốt bằng metric **Precision / Recall / F1-score** trên tập benchmark PII test suite, kết hợp với các quy tắc Data Quality Check tự động ở tầng Silver để quarantine các dòng nghi ngờ rò rỉ PII trước khi ghi xuống Gold.

## 5. Output (dán nguyên văn)

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.87s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ Push-Location dbt_project; try { ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17 } finally { Pop-Location }
09:29:35  Running with dbt=1.12.5
09:29:35  Registered adapter: duckdb=1.11.0
09:29:38  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
09:29:38  Concurrency: 1 threads (target='dev')
09:29:40  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.50 seconds (1.50s).
09:29:40  Completed successfully
09:29:40  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
