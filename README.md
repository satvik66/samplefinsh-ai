# FinShift AI

**Intelligent Enterprise Data Migration & Modernization Platform**

An independent, synthetic-data portfolio MVP demonstrating backend/data engineering, migration validation, incremental ETL, financial reconciliation, observability, and a grounded AI investigation copilot.

## Run locally

```bash
python -m venv .venv
# macOS/Linux: source .venv/bin/activate
# Windows: .venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

Use the sidebar to generate synthetic legacy data, run a full migration, and inspect rejects and reconciliation. Then try incremental migration. The optional OpenAI key in the sidebar enables generated explanations; otherwise the assistant provides evidence-based offline diagnostics.

## Architecture

```text
Synthetic legacy SQLite transactions (financial institution simulation)
       │
       ▼
Batch extraction / updated_at incremental watermark
       │
       ▼
Bronze (raw persisted copy) ──► validation ──► rejection quarantine
                                    │
                                    ▼
                            Silver (validated upserts)
                                    │
                                    ▼
                         Gold (daily financial mart)
                                    │
                     ┌──────────────┴──────────────┐
                     ▼                             ▼
               Reconciliation                Streamlit dashboard
                     │                             │
                     └──────────► AI Copilot ◄─────┘
```

## Stack

- **Python/SQLite:** runnable extraction, transactional upsert, validation, warehouse layers, run logging.
- **Streamlit/Pandas:** interactive control center, validation details, financial analytics.
- **OpenAI Responses API:** optional evidence-grounded copilot; no automatic data changes.
- **Unit tests:** full-load correctness, idempotency, incremental update.

## Extension architecture (not implemented in this MVP)

- Legacy Oracle or SQL Server via connectors and batch/CDC ingestion.
- AWS S3 raw and curated buckets; EMR/PySpark or AWS Glue distributed transformations.
- Airflow orchestration with retries, checkpoints, partition-aware backfills.
- Snowflake landing, MERGE, RBAC, secure financial marts.
- Terraform-managed infrastructure, GitHub Actions CI, IAM/KMS, lineage, OpenTelemetry.
- Schema mapping review and AI-assisted migration SQL conversion in an approval-only workflow.

## Accuracy notes

- Data are 100% synthetic; nothing comes from a real bank or employer.
- Current implementation uses a **local SQLite simulation**, not actual Oracle, Snowflake, S3, Spark, CDC, or distributed pipelines.
- The incremental watermark reads `updated_at > last_updated_at`; production deployments should use a compound `(updated_at, source_id)` cursor or CDC log position to protect against same-timestamp late arrivals.
- Validation rejects null account keys, negative amounts, or non-USD currency; rejected records remain inspectable in Bronze and quarantine logs.
- Source and target financial totals reconcile across the accepted/eligible source set, not all invalid source transactions.
- The AI assistant receives summarized evidence, not raw customer records; a real implementation would need access controls, redaction, evaluation, and audit logs.

## Run tests

```bash
python -m unittest discover -s tests -v
```

## Portfolio positioning

FinShift AI complements risk/ML demos by emphasizing reliable data movement, schema modernization, testability, observability, and trusted financial analytics. A production migration would require stronger connectors, reconciliation controls, deployment infrastructure, and scale testing.
