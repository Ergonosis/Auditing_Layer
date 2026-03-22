"""Reconciliation Agent - matches transactions across sources"""

from crewai import Agent, Task
from src.tools.reconciliation_tools import (
    cross_source_matcher,
    entity_resolver_kg,
    fuzzy_vendor_matcher,
    receipt_transaction_matcher,
    find_orphan_transactions
)
from src.agents.llm_config import get_shared_agent_llm

reconciliation_agent = Agent(
    role="Transaction Reconciliation Specialist",
    goal="Match transactions across credit card, bank, email, and receipt sources with 95%+ accuracy",
    backstory="""You are an expert financial auditor with 15 years of experience in reconciliation.
    You have a keen eye for spotting discrepancies and can match transactions even when vendor names vary.""",

    tools=[
        cross_source_matcher,
        entity_resolver_kg,
        fuzzy_vendor_matcher,
        receipt_transaction_matcher,
        find_orphan_transactions
    ],

    verbose=True,
    allow_delegation=False,
    llm=get_shared_agent_llm()
)

reconciliation_task = Task(
    description="""
    Match transactions across multiple sources and COMBINE with data quality results from previous task.

    **CRITICAL: You are the SECOND task in a 2-task pipeline. You must:**
    1. Access the context from the previous data_quality_task
    2. Perform your reconciliation work
    3. **COMBINE both results into ONE JSON output**

    **Reconciliation steps:**
    1. Use cross_source_matcher — pass transactions_json="[]", source_1="credit_card", source_2="bank"
    2. Use find_orphan_transactions — pass transactions_json="[]", all_sources_json='["credit_card","bank"]'
    3. Extract unmatched transaction IDs from reconciliation results

    **CRITICAL OUTPUT FORMAT - Must return COMBINED results:**
    {
        "data_quality": {
            "quality_score": <from previous task context>,
            "incomplete_records": ["EXP_001", "EXP_002"],  // List of transaction ID STRINGS
            "duplicates": {"duplicate_count": 10, "duplicate_groups": [...]},
            "gate_passed": true
        },
        "reconciliation": {
            "matched_pairs": [],
            "unmatched_transactions": [  // MUST be list of OBJECTS with txn_id field
                {"txn_id": "EXP_001"},
                {"txn_id": "EXP_002"}
            ],
            "match_rate": 0.85,
            "total_unmatched_1": 100
        }
    }

    **IMPORTANT:**
    - unmatched_transactions MUST be array of objects with "txn_id" field, NOT just strings
    - Include BOTH data_quality AND reconciliation sections
    - Use context from previous task to get data_quality results
    - Ensure valid JSON with proper escaping and no trailing commas
    """,

    agent=reconciliation_agent,
    expected_output="JSON object with BOTH data_quality and reconciliation results, where unmatched_transactions is array of objects with txn_id field"
)
