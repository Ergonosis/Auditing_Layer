# Ergonosis – Data Auditing Layer

This layer sits downstream of the Data Unification Layer. It ingests canonicalized transaction/email/calendar links from the Unified Query Interface (UQI), runs 6 specialized CrewAI agents in a parallel-then-sequential pipeline to detect data quality issues, reconciliation mismatches, and statistical anomalies, and produces severity-graded flags with a full immutable audit trail.

**Project Spec:** [Ergonosis – Complete Data Monitoring Architecture](https://www.notion.so/Ergonosis-Complete-Data-Monitoring-Architecture-2fcd8bf5e864808bb1bbc109723ccf53?source=copy_link)

**Upstream repo:** [Ergonosis/Data-Unification-Layer](https://github.com/dylancc5/Data-Unification-Layer)

**Downstream:** Finance Team Dashboard (no repo yet)

---

## Where Results Live

> **Note:** Production Databricks writes are planned for V2. Schemas are defined in [`src/db/schemas.py`](src/db/schemas.py) but not yet wired up. Currently, flags are collected in-memory and surfaced via the demo JSON output or the benchmark test harness.

The planned output catalog is **`ergonosis.auditing`** on the Databricks workspace.

### Audit tables — planned for V2

| Table            | Contents                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------ |
| `flags`          | Per-transaction flags: severity (CRITICAL/WARNING/INFO), confidence score, explanation, evidence |
| `audit_trail`    | Immutable append-only log: agent name, tool called, execution time, LLM tokens used, cost        |
| `workflow_state` | Per-run state: status, current agent, completed agents, intermediate results                     |

### Querying results today (demo / test mode)

Flags are accessible in-memory during a run. The demo pipeline writes a JSON summary to stdout:

```bash
python scripts/run_demo.py
# → prints JSON: { "audit_run_id": "...", "flags": [...], "summary": {...} }
```

To run the full benchmark and capture flags across all four test datasets:

```bash
python tests/demo_testing.py
# → prints confusion matrix (TP/FP/TN/FN), precision, recall, F1 per dataset
```

---

## Running on GCP

### Manual trigger

There is no scheduled trigger. Execute the pipeline on demand:

```bash
gcloud run jobs execute ergonosis-auditing-pipeline \
  --region=us-central1 \
  --wait \
  --project=ergonosis
```

### Viewing logs

```bash
gcloud run jobs logs read ergonosis-auditing-pipeline \
  --region=us-central1 \
  --project=ergonosis \
  --limit=50
```

### Deploying a new image

Pushing to `main` does **not** auto-deploy. To deploy after a code change:

```bash
SHORT_SHA=$(git rev-parse --short HEAD)
gcloud builds submit \
  --config cloudbuild.yaml \
  --project=ergonosis \
  --substitutions=SHORT_SHA=$SHORT_SHA .
```

The build step clones the unification repo, builds the Docker image, pushes it to Artifact Registry, and updates the Cloud Run job automatically.

### Secrets

All credentials are stored in **GCP Secret Manager** under project `ergonosis`. The pipeline loads them automatically when deployed via `cloudbuild.yaml`.

| Secret name              | Description                                   |
| ------------------------ | --------------------------------------------- |
| `ANTHROPIC_API_KEY`      | Anthropic API key for Claude (LLM calls)      |
| `DATABRICKS_HOST`        | Databricks workspace hostname                 |
| `DATABRICKS_TOKEN`       | Databricks personal access token              |
| `DATABRICKS_HTTP_PATH`   | SQL warehouse HTTP path                       |
| `UNIFICATION_USER_EMAIL` | Mailbox used to derive the UQI `user_id_hash` |

---

## Local Development & Testing

### Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # fill in ANTHROPIC_API_KEY, or use demo mode below
```

The unification repo must be installed as a sibling. For local dev, clone it alongside:

```bash
git clone https://github.com/dylancc5/Data-Unification-Layer ../ergonosis_unification
pip install -e ../ergonosis_unification
```

The Dockerfile handles this automatically for GCP builds.

### Running in demo mode (no credentials needed)

Demo mode uses local CSV fixtures from `ria_data/` and an in-memory SQLite store — no Databricks or real API calls required.

```bash
python scripts/run_demo.py
```

Four fixture datasets are available:

| Dataset                   | Contents                                   | Use case                        |
| ------------------------- | ------------------------------------------ | ------------------------------- |
| `clean_data/`             | Pristine transactions                      | Baseline / false-positive check |
| `missing_fields_15pct/`   | 15% missing vendor/amount/date fields      | Data quality detection          |
| `duplicates_10pct/`       | 10% duplicate transactions                 | Reconciliation detection        |
| `orphan_transactions_60/` | 60 unmatched transactions (no bank record) | Anomaly / orphan detection      |

### Running the test suite

```bash
python -m pytest tests/
```

All tests use in-memory SQLite. No real API calls are made regardless of local credentials.

### Running the benchmark harness

```bash
python tests/demo_testing.py
```

Runs the full pipeline across all four datasets and prints a confusion matrix (TP/FP/TN/FN), precision, recall, and F1 per dataset. Expected results: duplicates ~98% F1, missing_fields ~99% F1, orphan ~100% F1, clean_data F1 undefined (no corrupted rows).

### Run types

| Type         | Behaviour                                                    |
| ------------ | ------------------------------------------------------------ |
| `demo`       | Local CSV fixtures, in-memory state, no credentials required |
| `production` | Reads from Databricks via UQI, requires all GCP secrets set  |

---

## Known Limitations (V1)

- **Databricks output not yet wired:** Flags and audit logs remain in-memory during a run. The write layer to `ergonosis.auditing` is planned for V2. Schemas are ready in [`src/db/schemas.py`](src/db/schemas.py).

- **No scheduled trigger:** The pipeline runs on demand only. No Cloud Scheduler is configured.

- **Single-user / single-account:** Inherits the single Plaid account and single MS Graph mailbox constraint from the unification layer V1.

- **Sibling repo dependency:** Local development and Docker builds require `ergonosis_unification` to be available. The `Dockerfile` and `cloudbuild.yaml` handle this automatically; local dev requires a manual clone (see setup above).

- **Auto-tuning without human approval:** The weekly `scripts/feedback_analyzer.py` job adjusts detection rules autonomously (ADR-003 design decision).
