# Capstone: Assemble Your Mini-Lakehouse (SDA-DSC-214, Day 5)

Time: about 2 hours build, 30 minutes review. Work in pairs, one repo per trainee.
Covers LO7 (build a working Mini-Lakehouse), with LO3, LO4, LO6, LO8 touched.

Rule: every step is a variation of a tutorial you already ran. Nothing here is new.
Items marked <VERIFY> are filled in after a live run in the Codespace.

## 1. Starter layout (already in this repo)

You build inside the existing packages. Nothing new to install.

```
src/masar/ingest/land_bronze.py     step 1 (T1)
dbt/masar/models/marts/silver/      step 2 (T3, T7)
src/masar/delta/                    step 3 (T4)
src/masar/serve/                    steps 4-5 (T8, T9)
src/masar/pipeline.py               step 6: TODO skeleton, run with: make pipeline
capstone/checks/expected_counts.md  trainer's expected outputs per step
```

Fallback: branch `capstone-solution` holds a working version. A stuck trainee may
copy ONE step from it, mark it "borrowed" in the README, and move on.

## 2. Trainee checklist

Run each step, then compare the output with the checkpoint. Tick it only when it matches.

| # | Step | Based on | Checkpoint |
|---|------|----------|------------|
| 1 | Land bronze | T1 | Bronze row count = <VERIFY> |
| 2 | Build silver with dbt and the quality gate | T3, T7 | Silver row count = <VERIFY>; dbt tests pass |
| 3 | Add one Delta constraint and one schema change | T4 | Bad insert is rejected; new column visible |
| 4 | Build one gold mart | T8 | Gold row count = <VERIFY> |
| 5 | Build a consumer table (BI-safe view OR training set) | T9 | No PII columns; leakage check passes |
| 6 | Run everything with one command | T8 | run_all.sh exits 0 from a clean state |
| 7 | Prove it: counts per layer and one time-travel query | T5 | Version N and N-1 counts differ as expected |
| 8 | Write the README and submit | SUBMIT.md | Template filled, branch pushed |

Stretch (optional): second gold mart; quarantine table for null/duplicate trip_id.

Reminders: one Spark session at a time. If a step hangs, close other terminals running Spark.

## 3. Grading rubric (100 points)

| Area | Points | Full marks when |
|------|--------|-----------------|
| Pipeline runs end to end | 25 | run_all.sh succeeds from clean state |
| Bronze to silver correctness | 15 | Counts match checkpoints; quality gate present |
| Delta controls | 15 | Constraint blocks bad data; schema change applied safely |
| Gold and consumer table | 15 | Mart correct; consumer table has no PII or leakage |
| Proof (counts, time travel) | 10 | Counts per layer shown; time-travel query correct |
| README and explanation | 15 | Trainee explains each layer and one design choice in own words |
| Borrowed steps | -5 each | Deduct per step copied from the fallback branch, max -15 |
| Stretch | +5 | Any one stretch item working |

Pass mark: 60. Partial credit is expected; weaker trainees should reach 60-75 by
completing steps 1-6.

## 4. Trainer timing

| Minutes | Activity |
|---------|----------|
| 0-10 | Brief: goal, checklist, rules |
| 10-100 | Build, trainer circulates; checkpoint at steps 2, 4, 6 |
| 100-120 | Finish README, commit, submit |
| 120-150 | Pair review: explain your pipeline to your partner; trainer Q&A |

## 5. Trainer notes

- Known open items (do NOT expect trainees to fix; mention only as discussion):
  erased trips returning after a bronze re-merge; quarantine write not idempotent.
- Verify every <VERIFY> count in the Codespace before the course.
