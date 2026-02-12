# HUDI + AWS Glue SCD2 (Simple Pattern)

## Can we do SCD2 in Hudi?
Yes. Apache Hudi supports Slowly Changing Dimension Type 2 (SCD2) very well when you model each business key as multiple historical rows over time:

- One **current** row (`rec_cur_indicator = 'Y'`)
- Zero or more **historical** rows (`rec_cur_indicator = 'N'`)

Hudi does not have a built-in “SCD2 switch”, but with:
- deterministic record keys,
- upserts,
- precombine ordering,
- and a two-step update strategy,

you can implement reliable SCD2 semantics.

---

## 1) Target table model
Use a Hudi table in S3 with this shape (example):

| Column | Type | Purpose |
|---|---|---|
| `customer_id` | string | Business key |
| `name` | string | Dimension attribute |
| `city` | string | Dimension attribute |
| `effective_start_ts` | timestamp | Version start time |
| `effective_end_ts` | timestamp | Version end time (null or high date for current) |
| `rec_cur_indicator` | string | `'Y'` current, `'N'` expired |
| `version_id` | string | Unique per version (`customer_id` + start ts) |
| `op_ts` | timestamp | Event/update timestamp for precombine |
| `batch_id` | string | Optional traceability |

### Recommended keys
- `hoodie.datasource.write.recordkey.field = version_id`
- `hoodie.datasource.write.precombine.field = op_ts`
- `hoodie.datasource.write.partitionpath.field = city` (or business-friendly partition)

> Why `version_id` as record key?
> Because SCD2 requires multiple records per business key. If record key is only `customer_id`, each write overwrites history.

---

## 2) SCD2 business rules
For each incoming source row by `customer_id`:

1. Find current target record (`rec_cur_indicator = 'Y'`).
2. If no current record exists → insert new row as current:
   - `effective_start_ts = op_ts`
   - `effective_end_ts = null`
   - `rec_cur_indicator = 'Y'`
3. If current exists and attributes are unchanged → no action (optional, if you want idempotent behavior).
4. If current exists and attributes changed:
   - expire old row:
     - `effective_end_ts = new_op_ts - small_delta`
     - `rec_cur_indicator = 'N'`
   - insert new row:
     - `effective_start_ts = new_op_ts`
     - `effective_end_ts = null`
     - `rec_cur_indicator = 'Y'`

This is the classic SCD2 “close old + open new” sequence.

---

## 3) Simple AWS Glue flow
A practical Glue Spark job can be built in these stages:

1. Read source snapshot / CDC feed.
2. Read current target slice (`rec_cur_indicator = 'Y'`) from Hudi.
3. Join source vs current by business key.
4. Split into dataframes:
   - `new_records` (not matched)
   - `changed_records` (matched + attribute hash changed)
   - `unchanged_records` (ignored)
5. Build two write datasets:
   - `expired_updates` (old rows set to `N` with end timestamp)
   - `new_inserts` (new versions set to `Y`)
6. Union and write to Hudi via `upsert`.

### Why one upsert works
Because `version_id` differs between old and new rows:
- expired row uses **existing** `version_id` (updates that record)
- new row uses **new** `version_id` (inserts new version)

---

## 4) Example timeline
Given `customer_id = 101`:

- 2024-01-01 arrives with city=Boston → insert current
- 2024-03-01 arrives with city=Seattle →
  - old Boston row becomes `N`, end=2024-03-01
  - new Seattle row becomes `Y`, start=2024-03-01

You can now query point-in-time history:
- “as of 2024-02-15” → Boston row
- “current” → Seattle row (`rec_cur_indicator='Y'`)

---

## 5) Minimal Hudi write options (Glue)
Use Copy-on-Write first for simplicity:

- `hoodie.table.type = COPY_ON_WRITE`
- `hoodie.datasource.write.operation = upsert`
- `hoodie.datasource.write.recordkey.field = version_id`
- `hoodie.datasource.write.precombine.field = op_ts`
- `hoodie.datasource.write.partitionpath.field = city`
- `hoodie.datasource.hive_sync.enable = true`
- `hoodie.datasource.hive_sync.database = <glue_db>`
- `hoodie.datasource.hive_sync.table = <table_name>`
- `hoodie.datasource.hive_sync.mode = hms`

Later, if workload grows, you can evaluate MOR.

---

## 6) SQL query patterns
### Current records
```sql
SELECT *
FROM dim_customer
WHERE rec_cur_indicator = 'Y';
```

### Point-in-time (as-of)
```sql
SELECT *
FROM dim_customer
WHERE customer_id = '101'
  AND effective_start_ts <= TIMESTAMP '2024-02-15 00:00:00'
  AND (effective_end_ts > TIMESTAMP '2024-02-15 00:00:00' OR effective_end_ts IS NULL);
```

### Full history
```sql
SELECT *
FROM dim_customer
WHERE customer_id = '101'
ORDER BY effective_start_ts;
```

---

## 7) Common pitfalls to avoid
1. **Wrong record key**: using only business key destroys history.
2. **No change detection**: every run creates unnecessary versions.
3. **Clock issues**: `op_ts` must be stable/ordered enough for precombine logic.
4. **Concurrent writers**: start with one pipeline writer until locking strategy is configured.
5. **Late-arriving data**: define policy (insert back-dated version vs reject vs correction job).

---

## 8) Recommended “simple first” scope
Start with:
- Single dimension table
- Daily batch source
- One Glue job
- COW table
- `rec_cur_indicator` + effective dates
- Attribute hash for change detection

After that, add:
- CDC input
- late-data handling
- quality checks and reconciliation dashboards

