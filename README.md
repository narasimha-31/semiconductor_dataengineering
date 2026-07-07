# Semiconductor Supply Chain Intelligence Platform

**Ask a question in English. Get an answer computed from 16 years of US government data.**

> *"What happened to memory chip imports after the October 2022 export controls?"*
>
> **Bot:** Memory chip imports fell sharply after the October 2022 export controls. The 3-month average dropped from $280.3M before to $197.0M after — a **−29.7%** change.

That answer wasn't retrieved from a document. It was computed live — by a text-to-SQL chatbot querying a BigQuery serving layer, fed by a Kafka → PostgreSQL → dbt pipeline that ingests three US government APIs on a schedule, validates every record at four layers, and reconciles itself against the government's own totals to 0.00% variance.

This repo is that whole machine.

---

## The Problem

In 2021, a shortage of chips — parts that often cost less than a coffee — froze production lines across 169 industries and cost the auto industry alone an estimated $110B+. In October 2022, a single regulatory document from the Bureau of Industry and Security rewired the global semiconductor trade overnight.

Companies pay platforms like Resilinc and Interos six figures a year to answer three questions:

1. **How dependent are we on a single country for critical components?**
2. **When the government changes export rules, what actually happens to trade flows?**
3. **What are the key suppliers' financials signaling?**

All three are answerable from *free, public* US government data — if someone builds the pipeline. So I built it.

## The Solution

```
US Census Trade API ──►  Kafka producer (JSON Schema validation, DLQ)
Federal Register API ─►  Kafka producer      │
SEC EDGAR XBRL ───────►  Kafka producer      ▼
                         Kafka (KRaft, dual listeners)
                              │  idempotent consumers
                              ▼
                    PostgreSQL BRONZE  (raw, append-only evidence locker)
                              │  Great Expectations gate + SQL transforms
                              ▼
                    PostgreSQL SILVER  (typed, filtered, reconciled, DLQ'd)
                              │  dbt (7 models, 14 tests, lineage docs)
                              ▼
                    PostgreSQL GOLD    (HHI, regulatory windows, company signals)
                              │  Airflow-scheduled sync, row-count verified
                              ▼
                    GCP BigQuery  ◄──  Grok text-to-SQL chatbot (Streamlit)
                                       SELECT-only · table allowlist · LIMIT bolt-on

        6 Airflow DAGs orchestrate every hop · GitHub Actions CI on every PR
```

**Every tool has a job it alone does.** Kafka decouples flaky government APIs from the database and enables replay. Bronze stores exactly what the API sent (all TEXT — the evidence locker). Silver is where types, filters, and quarantine happen. dbt owns Silver→Gold with version-controlled, tested SQL. Great Expectations gates Bronze promotion with statistical checks dbt tests can't do. BigQuery serves the chatbot so the demo lives in the cloud. Nothing is here as a resume sticker.

## What It Found

| Finding | Evidence |
|---|---|
| **US processor-import concentration nearly quadrupled into the chip-shortage era** | HHI for HS 854231: 1,037 (2010, competitive) → 4,718 (2020, highly concentrated by DOJ thresholds) → ~2,233 (2026, diversifying) |
| **The Oct 13, 2022 export controls preceded a measurable trade collapse** | Memory chips (854232): **−29.7%** in the 3 months after vs. before. Processors: −13.2%. Follow-on Dec 2022 rule: −25%. |
| **The policy signal drowns when demand surges** | By April 2024, processor imports were **+34%** despite new restrictions — the AI boom overwhelming regulation. Correlation windows, honestly labeled: this platform surfaces the correlation; the analyst judges causation. |
| **Memory is today's most concentrated category** | HHI 3,330 (854232) vs 2,233 for processors — surfaced live by the chatbot |

## Proof It Works (Not Just "It Ran")

These are the receipts I'd show in a design review:

**Exactly-once, demonstrated on demand.** I reset the consumer group's offsets and replayed the entire topic: 76,311 messages re-consumed, **0 inserted, 76,311 duplicates rejected** by the unique constraint. At-least-once delivery + idempotent writes = effectively exactly-once — proven, not claimed.

**Self-auditing to 0.00%.** The Census API publishes a TOTAL row per code-month. Instead of discarding it, Silver stores it in a reconciliation table and an hourly Airflow check compares `SUM(countries)` against it. Result across all **980 code-months since 2010: 0.00% variance.** The pipeline proves it lost nothing, every hour.

**The quality gate fails on purpose.** I corrupted one Bronze value (`'CORRUPTED'` where a number belonged). The Great Expectations checkpoint went red naming the exact expectation, blocked Silver promotion, and after restoring the true value from the API, went green. A gate that has never failed is decoration.

**The chatbot can't be weaponized.** Generated SQL passes a guardrail: SELECT-only, forbidden-keyword scan (including mutations smuggled after semicolons), table allowlist, LIMIT bolt-on — all covered by 8 pytest cases running in CI. Ask it to `Delete all the trade data` and it refuses at two independent layers.

**CI blocks bad merges.** Every PR runs ruff, sqlfluff, 13 pytest cases, and `dbt parse` on GitHub's machines. Branch protection makes a red X physically unmergeable. The linter once caught a real runtime bug (three undefined names from an incomplete refactor) before I did.

## The Best Bug

The chatbot answered *"Which company leads the semiconductor business?"* with **"NVDA: $215.9B revenue."** Impressive — and wrong. That's NVIDIA's full fiscal year sitting next to other companies' single quarters.

Tracing it back: SEC XBRL facts carry a `start` and `end` date, and my EDGAR producer had kept only `end` — silently mixing quarterly, nine-month year-to-date, and annual values in one column. The fix ran through all six layers: duration classification at the producer (75–105 days = quarter, 350–380 = fiscal year, YTD discarded), a `period_type` column through Bronze and Silver, widened unique constraints, a re-pivoted dbt mart with honestly separated `revenue_usd` (quarterly) and `revenue_fy_usd` columns, a re-synced serving layer, and an updated chatbot schema prompt.

The serving layer caught a bug in the ingestion layer. That's what end-to-end ownership looks like — and it's my favorite thing in this repo.

## Honest Limitations

- **Correlation ≠ causation.** The regulatory-impact windows measure what trade *did* around a rule, not what the rule *caused*. The 2024 data proves confounders exist.
- **TSM and GFS report under IFRS** with no matching capex tags (documented coverage gap) and file annually — their data lives in the FY column.
- **Census data is country-level** — firm-level trade is confidential, which is exactly why SEC EDGAR is a separate source.
- **Dev-environment shortcuts, named:** no Airflow Fernet key (connection secrets unencrypted at rest), three near-identical consumers awaiting a refactor into one parameterized module.

## Numbers

| | |
|---|---|
| Trade records (2010-01 → 2026-04, 5 HS codes, 215 countries) | **76,311** |
| BIS regulatory documents (2002 → present) | **2,968** |
| Company financial facts (10 tickers, Q/FY/INSTANT classified) | **1,416** |
| dbt models / tests | 7 / 14 |
| Airflow DAGs | 6 |
| pytest cases in CI | 13 |
| Reconciliation variance | **0.00%** |
| Total cash spent | ~$10 (xAI credits; GCP free tier + $1 budget alarm) |

## Run It

```bash
cp .env.example .env          # fill in your keys (Census, SEC user agent, GCP, xAI)
docker compose up -d           # Kafka, PostgreSQL, Airflow (custom image, deps baked in)
# Airflow UI: http://localhost:8080 — unpause the DAGs
streamlit run chatbot/app.py   # the serving layer
```

Secrets live in `.env` and `secrets/` — both gitignored from the first commit that needed them. (Lesson learned the hard way in week one; the key that leaked was revoked and history rewritten.)

## How This Was Built

I come from a data-analyst background; this project was my deliberate crossing into data engineering. I built it with AI assistance as a pair programmer and tutor — every command was run by me, every failure debugged by me (SIGTERM forensics at midnight, a git history untangling, an XCom contract break, the fiscal-period hunt), and every architectural decision — recall-over-precision on the regulatory filter, truncate-and-reload over incremental sync, scoped GE instead of tool sprawl — is one I made and can defend without an AI in the room.

I think that's simply how engineering works now. The judgment is the job; the typing never was.
