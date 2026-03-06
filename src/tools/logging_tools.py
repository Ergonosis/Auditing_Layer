"""Audit logging tools for compliance and transparency"""

from crewai.tools import tool
from typing import Dict, Any, List
from datetime import datetime
from src.utils.logging import get_logger
import json

logger = get_logger(__name__)

# In-memory audit trail (in production, this would write to database)
AUDIT_TRAIL = []


@tool("log_agent_decision")
def log_agent_decision(
    agent_name: str,
    action: str,
    input_data: dict[str, Any],
    output_data: dict[str, Any],
    metadata: dict[str, Any]
) -> str:
    """
    Log agent decision with full context

    Args:
        agent_name: Name of agent
        action: Action/tool called
        input_data: Input parameters
        output_data: Output results
        metadata: Additional metadata (execution time, tokens, cost)

    Returns:
        Confirmation message
    """
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'agent_name': agent_name,
        'action': action,
        'input_data': input_data,
        'output_data': output_data,
        'metadata': metadata
    }

    AUDIT_TRAIL.append(log_entry)
    logger.info(f"Logged {agent_name} decision", action=action)

    return f"Decision logged for {agent_name}"


@tool("create_audit_trail_entry")
def create_audit_trail_entry(flag_id: str, decision_chain_json: str = "[]") -> str:
    """
    Link flag to decision chain

    Args:
        flag_id: Flag UUID
        decision_chain_json: JSON array string of decisions like '[{"agent": "DataQuality", "action": "check", ...}, ...]'

    Returns:
        Confirmation message
    """
    # Parse JSON string to list
    import json
    decision_chain = json.loads(decision_chain_json) if decision_chain_json else []

    entry = {
        'flag_id': flag_id,
        'decision_chain': decision_chain,
        'timestamp': datetime.now().isoformat()
    }

    AUDIT_TRAIL.append(entry)
    logger.info(f"Created audit trail for flag {flag_id}")

    return f"Audit trail created for flag {flag_id}"


@tool("get_audit_trail")
def get_audit_trail(audit_run_id: str) -> str:
    """
    Retrieve audit trail for specific run

    Args:
        audit_run_id: Audit run ID

    Returns:
        JSON string of audit trail entries
    """
    # In production, query from database
    # For now, return in-memory trail
    logger.info(f"Retrieved audit trail for run {audit_run_id}")
    return json.dumps(AUDIT_TRAIL, indent=2)


@tool("generate_lineage_trace")
def generate_lineage_trace(transaction_id: str) -> dict:
    """
    Generate data lineage graph for transaction

    Args:
        transaction_id: Transaction ID

    Returns:
        Lineage graph structure
    """
    # Simplified lineage
    lineage = {
        'transaction_id': transaction_id,
        'lineage': [
            {'stage': 'Bronze.credit_cards', 'row_id': 'row_456'},
            {'stage': 'Silver.transactions_cleaned', 'row_id': 'row_789'},
            {'stage': 'Gold.transactions_enriched', 'row_id': 'row_123'},
            {'stage': 'Audit.flagged', 'flag_id': 'flag_xxx'}
        ]
    }

    logger.info(f"Generated lineage trace for transaction {transaction_id}")
    return lineage


def clear_audit_trail():
    """Clear the audit trail (for testing)"""
    global AUDIT_TRAIL
    AUDIT_TRAIL = []
    logger.info("Audit trail cleared")


# HumanDecision → FeedbackSignal mapping
_DECISION_TO_SIGNAL = {
    "approved": "confirmed",
    "rejected": "rejected",
    "false_positive": "rejected",
    "needs_more_info": "flagged",
}


def record_human_decision(flag_id: str, transaction_id: str, human_decision: str) -> bool:
    """Write a feedback signal to the unification layer when a human reviews a flag.

    This is NOT a CrewAI tool — it is called by whatever review workflow
    sets the human_decision field on a flag.

    Args:
        flag_id: The flag being reviewed.
        transaction_id: The transaction the flag refers to.
        human_decision: One of HumanDecision enum values
                        (approved, rejected, false_positive, needs_more_info).

    Returns:
        True if feedback was written, False otherwise.
    """
    signal = _DECISION_TO_SIGNAL.get(human_decision)
    if signal is None:
        logger.warning(
            "Unknown human_decision value — skipping feedback write",
            flag_id=flag_id,
            human_decision=human_decision,
        )
        return False

    from src.integrations.unification_client import try_write_feedback
    result = try_write_feedback(
        transaction_id=transaction_id,
        signal=signal,
        source="human",
        reason=f"flag={flag_id} decision={human_decision}",
    )

    logger.info(
        "Human decision feedback recorded",
        flag_id=flag_id,
        transaction_id=transaction_id,
        human_decision=human_decision,
        signal=signal,
        written=result,
    )
    return result
