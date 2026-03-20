# Ergonosis – Data Auditing Layer

This layer sits downstream of the Data Unification Layer. It ingests canonicalized transaction/email/calendar links from the Unified Query Interface (UQI), runs 6 specialized CrewAI agents in a parallel-then-sequential pipeline to detect data quality issues, reconciliation mismatches, and statistical anomalies, and produces severity-graded flags with a full immutable audit trail.

**Project Spec:** [Ergonosis – Complete Data Monitoring Architecture](https://www.notion.so/Ergonosis-Complete-Data-Monitoring-Architecture-2fcd8bf5e864808bb1bbc109723ccf53?source=copy_link)

**Upstream repo:** [Ergonosis/Data-Unification-Layer](https://github.com/dylancc5/Data-Unification-Layer)

**Downstream:** Finance Team Dashboard (no repo yet)

---

## Where Results Live

In production, flags and workflow state are written to the **`ergonosis.auditing`** catalog on the Databricks workspace in real time. In demo/dev mode, flags are collected in-memory and surfaced via the demo JSON output or the benchmark test harness.

### Audit tables

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

### Automatic trigger (1 hour after unification)

The auditing pipeline is chained to the unification pipeline via **GCP Workflows** with a 1-hour delay. The workflow definition lives at [`workflows/audit_trigger.yaml`](workflows/audit_trigger.yaml).

The workflow is triggered daily at **6am UTC** by Cloud Scheduler job `ergonosis-full-pipeline-nightly`. It: (1) executes `ergonosis-unification-pipeline`, (2) waits 1 hour, (3) executes `ergonosis-auditing-pipeline`.

**Deploy or update the workflow:**

```bash
gcloud workflows deploy ergonosis-audit-trigger \
  --location=us-central1 \
  --source=workflows/audit_trigger.yaml \
  --project=ergonosis
```

**Monitor a workflow execution:**

```bash
gcloud workflows executions list ergonosis-audit-trigger \
  --location=us-central1 \
  --project=ergonosis \
  --limit=5
```

### Manual trigger

To run the auditing pipeline immediately (bypassing the workflow):

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

Pushing to `main` does **not** auto-deploy. The build requires the unification repo to be present in the source upload. To deploy after a code change:

```bash
cp -r ../ergonosis_unification ./ergonosis_unification_src
SHORT_SHA=$(git rev-parse --short HEAD)
gcloud builds submit --config cloudbuild.yaml --project=ergonosis --substitutions=SHORT_SHA=$SHORT_SHA .
rm -rf ./ergonosis_unification_src
```

The Cloud Run job runs as `ergonosis-pipeline-sa@ergonosis.iam.gserviceaccount.com` (same SA as the unification job). Task timeout is **3 hours**.

### Secrets

All credentials are stored in **GCP Secret Manager** under project `ergonosis`. Secrets are loaded at **runtime** via `load_secrets_to_env()` in `src/main.py` — the same pattern as the unification layer. `cloudbuild.yaml` does **not** use `--update-secrets`; do not add it back, as it requires extra IAM grants that aren't needed with the runtime loading approach.

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

| Type         | Behaviour                                                                                  |
| ------------ | ------------------------------------------------------------------------------------------ |
| `demo`       | Local CSV fixtures, in-memory state, no credentials required                               |
| `production` | Reads from Databricks via UQI, writes flags/state to `ergonosis.auditing`, requires all GCP secrets set |

---

## Known Limitations

- **Single-user / single-account:** Inherits the single Plaid account and single MS Graph mailbox constraint from the unification layer V1.

- **Sibling repo dependency:** Local development and Docker builds require `ergonosis_unification` to be available. The `Dockerfile` and `cloudbuild.yaml` handle this automatically; local dev requires a manual clone (see setup above).

- **Auto-tuning without human approval:** The weekly `scripts/feedback_analyzer.py` job adjusts detection rules autonomously (ADR-003 design decision).
