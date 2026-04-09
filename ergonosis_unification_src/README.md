# Ergonosis – Data Unification Layer

This layer sits between the upstream aggregation layer (Plaid + Microsoft Graph) and the downstream CrewAI auditing layer. It canonicalizes raw extracted records, runs a three-tier deterministic matching cascade to link entities across types (transactions ↔ emails ↔ calendar events), and exposes a governed query interface to the auditing layer.

**Project Spec:** [Ergonosis – Data Unification Layer Proposal](https://www.notion.so/Ergonosis-Data-Unification-Layer-Proposal-316d8bf5e8648036be71da4cdca63723)

**Upstream repo:** [Ergonosis/Data-Aggregation-Unifying-Layer](https://github.com/Ergonosis/Data-Aggregation-Unifying-Layer)

**Downstream repo:** [Ergonosis/Auditing_Layer](https://github.com/Ergonosis/Auditing_Layer)

---

## Where Results Live

All output is stored in the **Databricks catalog `ergonosis.unification`**, accessible via the SQL warehouse at `/sql/1.0/warehouses/2bb91a9fc7b4b342` on `8259562201429934.4.gcp.databricks.com`.

### Silver Tables — canonical raw records

| Table             | Contents                                                                |
| ----------------- | ----------------------------------------------------------------------- |
| `transactions`    | Plaid transactions: amount, date, merchant_name, category               |
| `emails`          | MS Graph emails: sender, recipients, subject, body_preview, thread_id   |
| `calendar_events` | MS Graph calendar events: organizer, subject, attendees, start/end time |

### Gold Tables — relationship outputs

| Table                | Contents                                                               |
| -------------------- | ---------------------------------------------------------------------- |
| `entity_links`       | Confirmed cross-entity links with match_tier, confidence, rule_version |
| `unmatched_entities` | Entities that fell through all matching tiers (V2 input queue)         |
| `ambiguous_matches`  | Multiple candidates above threshold — need resolution                  |
| `link_feedback`      | Signals from auditing agents (confirmed / rejected / flagged)          |
| `run_log`            | Pipeline run history: status, counts, timestamps                       |
| `intent_log`         | Write-ahead audit log of all planned operations (TTL: 24h)             |
| `watermarks`         | Incremental sync cursors per entity type                               |

### Querying results programmatically

Use `UnifiedQueryInterface` from `src/query_interface.py`. All methods require an active `data_processing` consent and a `user_id_hash` (SHA-256 of the user email).

```python
from src.query_interface import UnifiedQueryInterface
from src.storage.delta_client import get_storage_backend

qi = UnifiedQueryInterface(get_storage_backend())
user_id_hash = "91da4b72..."  # SHA-256 prefix of user email

# Get confirmed links for an entity
qi.get_linked_entities("txn_123", "transaction", user_id_hash=user_id_hash)

# Get unmatched entities (V2 input queue)
qi.get_unlinked_entities("transaction", user_id_hash=user_id_hash)

# Get ambiguous matches awaiting resolution
qi.get_ambiguous_matches(status="pending", user_id_hash=user_id_hash)

# Submit feedback on a link
qi.write_feedback("link_abc", signal="confirmed", source="autonomous", user_id_hash=user_id_hash)

# Fetch last run status
qi.get_last_run_status()

# Fetch a raw Silver record by entity ID
qi.get_entity("txn_123", "transaction", user_id_hash=user_id_hash)
```

You can also query directly in the Databricks SQL editor:

```sql
SELECT * FROM ergonosis.unification.entity_links ORDER BY created_at DESC LIMIT 100;
SELECT COUNT(*) FROM ergonosis.unification.transactions;
SELECT * FROM ergonosis.unification.run_log ORDER BY started_at DESC LIMIT 10;
```

---

## Running on GCP

### Scheduled runs

Two schedulers run daily at **6am UTC**:

- **`ergonosis-full-pipeline-nightly`** — triggers the GCP Workflow `ergonosis-audit-trigger`, which runs unification → waits 1 hour → runs the auditing pipeline. This is the primary nightly scheduler.
- **`ergonosis-incremental-daily`** — triggers unification only (no auditing). Kept as a fallback.

No manual action needed for normal operation.

### Manual trigger

```bash
gcloud run jobs execute ergonosis-unification-pipeline \
  --region=us-central1 \
  --wait \
  --project=ergonosis
```

### Viewing logs

```bash
gcloud run jobs logs read ergonosis-unification-pipeline \
  --region=us-central1 \
  --project=ergonosis \
  --limit=50
```

### Deploying a new image

Pushing to `main` does **not** auto-deploy — Cloud Build triggers are not configured. To deploy after a code change:

```bash
SHORT_SHA=$(git rev-parse --short HEAD)
gcloud builds submit \
  --config cloudbuild.yaml \
  --project=ergonosis \
  --substitutions=SHORT_SHA=$SHORT_SHA .
```

The build step also updates the Cloud Run job image automatically.

### Secrets

All credentials are stored in **GCP Secret Manager** under project `ergonosis`. The pipeline loads them automatically when `GCP_PROJECT_ID` is set.

| Secret name                    | Description                                  |
| ------------------------------ | -------------------------------------------- |
| `DATABRICKS_HOST`              | Databricks workspace hostname                |
| `DATABRICKS_TOKEN`             | Databricks personal access token             |
| `DATABRICKS_HTTP_PATH`         | SQL warehouse HTTP path                      |
| `PLAID_ACCESS_TOKEN`           | Plaid access token for the client account    |
| `PLAID_CLIENT_ID`              | Plaid client ID                              |
| `PLAID_SECRET`                 | Plaid secret                                 |
| `PLAID_ENV`                    | Plaid environment (`sandbox` / `production`) |
| `MSGRAPH_CLIENT_ID`            | Azure app registration client ID             |
| `MSGRAPH_CLIENT_SECRET`        | Azure app registration client secret         |
| `MSGRAPH_TENANT_ID`            | Azure tenant ID                              |
| `UNIFICATION_USER_EMAIL`       | Mailbox to pull from MS Graph                |
| `UNIFICATION_PLAID_ACCOUNT_ID` | Plaid account ID filter (optional)           |

---

## Local Development & Testing

### Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # fill in credentials, or use STUB_INGESTION below
```

### Running with stub data (no real credentials needed)

Three `STUB_INGESTION` modes are available:

| Value  | Data                             | Use case                   |
| ------ | -------------------------------- | -------------------------- |
| `true` | Basic fixtures: 6 txns, 6 emails | Fast local checks          |
| `rich` | Rich mock data with edge cases   | Matching logic development |
| `smb`  | SMB-vertical test data           | Business-vertical testing  |

```bash
# Full refresh with stub data — results go to local SQLite, no Databricks needed
STUB_INGESTION=true python -m src.pipeline --type full_refresh
```

### Running the test suite

```bash
python -m pytest tests/
```

All 398 tests use a fresh in-memory SQLite per test. No real API calls are made regardless of local credentials.

### Running against real Databricks locally

Fill in your `.env` with real credentials, then:

```bash
UNIFICATION_SECURE_STORAGE_REQUIRED=true python -m src.pipeline --type incremental
```

### Run types

| Type           | Behaviour                                                   |
| -------------- | ----------------------------------------------------------- |
| `incremental`  | Processes records since the last watermark (default on GCP) |
| `full_refresh` | Reprocesses all records from epoch; resets all watermarks   |

---

## Known Limitations (V1)

- **Single-user / single-account:** The pipeline ingests one Plaid account and one MS Graph mailbox per run. Multi-tenant support is planned for V2.

- **MS Graph system emails filtered:** Emails with no sender or empty recipients (Exchange system/internal messages) are automatically dropped before the transformer.

- **Auditing pipeline timeout:** The auditing layer (`ergonosis-auditing-pipeline`) has a 3h Cloud Run task timeout. With large transaction volumes it may still time out — batching support is planned.
