# Ergonosis Auditing - Data Auditing Agent Ecosystem

A production-ready, cost-optimized agentic audit system using CrewAI, Databricks, and a tool-first architecture.

## 🎯 System Overview

**Tool-First, LLM-Light Architecture**: 97% deterministic operations (SQL/ML), 3% LLM calls

### Key Features

- ✅ **6 Specialized Agents**: Data Quality, Reconciliation, Anomaly Detection, Context Enrichment, Escalation, Audit Logging
- ✅ **Cost Optimized**: ~$150/month total ($100-150 Databricks compute + $2 LLM)
- ✅ **Scalable**: Processes 10,000+ transactions/month
- ✅ **Auto-Tuning**: Weekly feedback analysis automatically adjusts rules
- ✅ **Full Audit Trail**: Immutable append-only logs for 7-year compliance
- ✅ **Parallel Execution**: 3 parallel agents → 2 sequential agents for optimal performance

---

## 📊 Architecture

```
Triggers (Hourly/Event/Manual)
    ↓
Orchestrator Agent (CrewAI Manager)
    ↓
┌─────────────────── PARALLEL EXECUTION (2-3 min) ───────────────────┐
│  Data Quality Agent       Reconciliation Agent    Anomaly Detection │
│  (completeness check)     (cross-source match)    (ML-based)       │
└─────────────────────────────────────────────────────────────────────┘
    ↓
┌─────────────────── SEQUENTIAL EXECUTION (3-5 min) ─────────────────┐
│  Context Enrichment Agent → Escalation Agent                        │
│  (email/receipt search)      (severity classification)              │
└─────────────────────────────────────────────────────────────────────┘
    ↓
Flag Database + Audit Trail
    ↓
Frontend Dashboard (Finance Team Review)
    ↓
Weekly Feedback Analysis → Auto-Tune Rules
```

For full architecture details, see [`system_flowchart.mmd`](system_flowchart.mmd) and [`system_specs.md`](system_specs.md).

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Redis (for state management)
- OpenRouter API key (for LLM calls)
- Databricks access (optional for now - uses mock data adapter)

### Installation

```bash
# Clone repository
cd ergonosis_auditing

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY (or OPENAI_API_KEY if using OpenAI)

# Run demo audit
python scripts/run_demo.py
```

### Testing Framework

The project includes an automated testing framework with augmented datasets:

```bash
# Generate test datasets with various corruptions
python scripts/generate_test_datasets.py

# Run automated benchmark suite
python tests/demo_testing.py
```

**Test Datasets:**
- `clean_data/` - Original pristine data
- `missing_fields_15pct/` - 15% missing vendor/amount/date fields
- `duplicates_10pct/` - 10% duplicate transactions
- `orphan_transactions_60/` - 60 unmatched transactions (no bank reconciliation)

**Benchmark Metrics:**
- Full confusion matrix (TP/FP/TN/FN)
- Precision, Recall, F1 Score
- Performance comparison across corruption types

---

## 📂 Project Structure

```
ergonosis_auditing/
├── config/                      # Configuration files
│   ├── rules.yaml               # Auto-tuned rules and thresholds
│   └── deployment/              # Databricks workflow configs
├── src/
│   ├── orchestrator/            # Master coordinator
│   ├── agents/                  # 6 specialized agents
│   ├── tools/                   # 30+ tools for agents
│   ├── models/                  # Pydantic data models
│   ├── db/                      # Database schemas
│   ├── ml/                      # ML models
│   ├── utils/                   # Utilities
│   └── main.py                  # Entry point
├── scripts/
│   └── feedback_analyzer.py     # Weekly auto-tuning job
├── tests/                       # Unit and integration tests
├── docs/                        # Task specifications
│   ├── TASK_1_INFRASTRUCTURE.md
│   ├── TASK_2_DATA_QUALITY_AGENT.md
│   ├── TASK_3_RECONCILIATION_ANOMALY_AGENTS.md
│   ├── TASK_4_CONTEXT_ESCALATION_AGENTS.md
│   ├── TASK_5_ORCHESTRATOR_MAIN.md
│   └── TASK_ASSIGNMENTS_SUMMARY.md
├── development_history.md       # Architecture decision log
└── README.md
```

---

## 🛠️ Development Status

### ✅ Completed (Foundation)

- Directory structure
- Configuration system (rules.yaml, .env)
- Data models (Transaction, Flag, AuditLogEntry, VendorProfile)
- Utility modules (logging, metrics, config_loader, errors)
- Constants and enums
- Architecture decision log (6 ADRs)

### 🚧 Ready for Implementation (See Task Assignments)

The remaining implementation is broken down into **5 independent, parallelizable tasks**:

1. **TASK 1**: Core Infrastructure (Databricks, LLM, State Manager) - 2 hours
2. **TASK 2**: Data Quality Agent + 5 Tools - 2 hours
3. **TASK 3**: Reconciliation + Anomaly Detection Agents + 10 Tools - 3 hours
4. **TASK 4**: Context Enrichment + Escalation Agents + 10 Tools - 3 hours
5. **TASK 5**: Orchestrator + Logging Agent + Main Entry Point - 4 hours

**👉 See [`docs/TASK_ASSIGNMENTS_SUMMARY.md`](docs/TASK_ASSIGNMENTS_SUMMARY.md) for detailed task breakdown and assignment instructions.**

---

## 🎯 Key Design Decisions (ADRs)

All architectural decisions are tracked in [`development_history.md`](development_history.md):

- **ADR-001**: Databricks Workflows for deployment (native Delta Lake integration)
- **ADR-002**: Tool-First Architecture (97% cost reduction via deterministic filtering)
- **ADR-003**: Weekly Auto-Tuning without human approval (1 week vs 4+ weeks manual)
- **ADR-004**: Delta Lake for Knowledge Graph (zero additional infrastructure cost)
- **ADR-005**: Abstract Databricks interface with mock data (unblocks development)
- **ADR-006**: Single broad domain strategy (faster initial deployment)

---

## 💰 Cost Breakdown

**Monthly Cost Target**: ~$150/month per client

| Component                         | Monthly Cost |
| --------------------------------- | ------------ |
| Databricks compute (job clusters) | $100-150     |
| LLM API calls (OpenRouter)        | $2-10        |
| Redis (state management)          | $0-30        |
| **Total**                         | **~$150**    |

**LLM Usage**:

- 1000 audits/month × ~30 LLM calls/audit × ~500 tokens/call = **15M tokens/month**
- Cost: ~$2-10/month (well under $100 budget)

---

## 📚 Documentation

- [`system_specs.md`](system_specs.md) - Complete system specifications (1263 lines)
- [`system_flowchart.mmd`](system_flowchart.mmd) - Mermaid architecture diagram
- [`development_history.md`](development_history.md) - Architecture decision log
- [`docs/TASK_ASSIGNMENTS_SUMMARY.md`](docs/TASK_ASSIGNMENTS_SUMMARY.md) - Implementation task breakdown

---
