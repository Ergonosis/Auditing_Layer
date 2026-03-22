"""Data Quality Agent - validates data before auditing"""

from crewai import Agent, Task
from src.tools.data_quality_tools import (
    check_data_completeness,
    validate_schema_conformity,
    detect_duplicate_records,
    infer_domain_freshness,
    check_data_quality_gates
)
from src.utils.logging import get_logger
from src.agents.llm_config import get_shared_agent_llm

logger = get_logger(__name__)

# Agent definition
data_quality_agent = Agent(
    role="Data Quality Specialist",
    goal="Validate data completeness, schema conformity, and freshness with 95%+ accuracy before audit begins",
    backstory="""You are an expert data engineer with 10 years of experience in data validation.
    You ensure that all transaction data meets quality standards before the audit process begins.
    You are meticulous about data integrity and never let bad data pass through.""",

    tools=[
        check_data_completeness,
        validate_schema_conformity,
        detect_duplicate_records,
        infer_domain_freshness,
        check_data_quality_gates
    ],

    verbose=True,
    allow_delegation=False,
    llm=get_shared_agent_llm()  # LLM orchestrates tools and formats JSON output
    # Individual tools may still call LLM via llm_client.py for specialized reasoning
)

# Task definition
data_quality_task = Task(
    description="""
    Given transaction data, validate quality before audit begins:

    1. **Check data completeness** - pass transactions_json={transactions} to check_data_completeness
    2. **Validate schema conformity** - pass transactions_json={transactions} to validate_schema_conformity
    3. **Detect duplicate records** - pass transactions_json={transactions} to detect_duplicate_records
    4. **Infer domain freshness** - use infer_domain_freshness (do NOT pass transactions to this tool)
    5. **Apply quality gates** - use check_data_quality_gates with metrics from step 1 (do NOT pass transactions to this tool)

    **Input**: The full transaction data is in `{transactions}` (JSON array string).
    Pass `{transactions}` as the `transactions_json` argument to tools in steps 1-3.
    **Output**: Structured report with:
    - quality_score (0-1)
    - incomplete_records (list of transaction ID STRINGS like ["EXP_001", "EXP_002"])
    - schema_violations (list of errors)
    - duplicates (dict with duplicate_count and duplicate_groups)
    - domain_config (dict with domain, max_age_hours)
    - freshness_violations (list of stale transactions)
    - gate_passed (bool - True if quality acceptable, False if audit should halt)

    **Important**:
    - Use tools sequentially in order listed above
    - incomplete_records MUST be a list of transaction ID STRINGS (e.g., ["EXP_001", "EXP_002"])
    - Extract txn_id values from tool results and return as simple string array
    - If quality gate fails (completeness <90%), still return full results
    - Ensure valid JSON with no trailing commas
    - This output will be passed to the next task via context
    """,

    agent=data_quality_agent,

    expected_output="""Valid JSON object with data quality report. incomplete_records MUST be array of ID strings:
    {
        "quality_score": 0.95,
        "incomplete_records": ["EXP_001", "EXP_002"],
        "schema_violations": [],
        "duplicates": {"duplicate_count": 3, "duplicate_groups": []},
        "domain_config": {"domain": "business_operations", "max_age_hours": 48, "confidence": 1.0},
        "freshness_violations": [],
        "gate_passed": true
    }
    """
)
