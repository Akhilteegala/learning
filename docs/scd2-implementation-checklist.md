# SCD2 on Hudi with Glue — Implementation Checklist

## Phase 1: Data model
- [ ] Confirm business key (`customer_id`)
- [ ] Add SCD2 columns:
  - [ ] `effective_start_ts`
  - [ ] `effective_end_ts`
  - [ ] `rec_cur_indicator`
  - [ ] `version_id`
  - [ ] `op_ts`
- [ ] Decide partition strategy

## Phase 2: Change detection
- [ ] Define tracked columns for SCD2 (e.g., `name`, `city`, `status`)
- [ ] Create deterministic hash of tracked columns
- [ ] Compare source hash vs current target hash
- [ ] Route rows into `new`, `changed`, `unchanged`

## Phase 3: SCD2 write logic
- [ ] Build expired rows from current target rows:
  - [ ] set `rec_cur_indicator = 'N'`
  - [ ] set `effective_end_ts = incoming_op_ts`
- [ ] Build new current rows:
  - [ ] set `rec_cur_indicator = 'Y'`
  - [ ] set `effective_start_ts = incoming_op_ts`
  - [ ] set `effective_end_ts = null`
  - [ ] set new `version_id`
- [ ] Upsert both datasets into same Hudi table

## Phase 4: Hudi + Glue configuration
- [ ] `operation = upsert`
- [ ] `recordkey = version_id`
- [ ] `precombine = op_ts`
- [ ] `table.type = COPY_ON_WRITE` (initial)
- [ ] Enable Glue/Hive sync

## Phase 5: Validation queries
- [ ] Exactly one current row per business key
- [ ] No overlapping effective date windows per business key
- [ ] Historical count increases only on real changes
- [ ] As-of queries return expected versions

## Phase 6: Operational hardening
- [ ] Add idempotency strategy for retries
- [ ] Add data quality checks (null keys, invalid timestamps)
- [ ] Add monitoring for write errors and lag
- [ ] Document late-arriving data policy

## Definition of done (simple MVP)
- [ ] End-to-end Glue job writes SCD2 history to Hudi
- [ ] `rec_cur_indicator` correctly marks current and expired rows
- [ ] Athena queries show both current and historical versions
- [ ] Backfill test proves “go back in time” reporting

