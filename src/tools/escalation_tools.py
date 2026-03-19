"""Escalation and severity classification tools"""

from crewai.tools import tool
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import uuid
import os
from datetime import datetime
from src.tools.llm_client import call_llm
from src.llm.provider import ANTHROPIC_SMART_MODEL
from src.utils.logging import get_logger
from src.utils.config_loader import load_config
from src.constants import SeverityLevel
import json

logger = get_logger(__name__)


# Pydantic input models for OpenAI function calling schema compliance
class TransactionInput(BaseModel):
    """Transaction details for tool operations"""
    txn_id: str = Field(description="Transaction ID")
    vendor: str = Field(description="Vendor name")
    amount: float = Field(description="Transaction amount")
    date: str = Field(description="Transaction date (ISO format)")
    source: str = Field(description="Data source (credit_card, bank, etc.)")
    account: Optional[str] = Field(None, description="Account identifier")
    category: Optional[str] = Field(None, description="Transaction category")
    description: Optional[str] = Field(None, description="Transaction description")


class DataQualityResult(BaseModel):
    """Data quality agent output structure"""
    quality_score: Optional[float] = Field(None, description="Overall quality score")
    incomplete: Optional[bool] = Field(False, description="Whether record is incomplete")
    incomplete_records: Optional[List[str]] = Field(default_factory=list, description="List of incomplete record IDs")
    duplicate_records: Optional[List[str]] = Field(default_factory=list, description="List of duplicate record IDs")


class ReconciliationResult(BaseModel):
    """Reconciliation agent output structure"""
    matched: Optional[bool] = Field(False, description="Whether transaction was matched")
    match_rate: Optional[float] = Field(None, description="Match rate percentage")
    suspicious_transactions: Optional[List[str]] = Field(default_factory=list, description="List of suspicious transaction IDs")


class AgentResultsInput(BaseModel):
    """Combined results from data_quality and reconciliation agents"""
    data_quality: DataQualityResult = Field(description="Data quality agent output")
    reconciliation: ReconciliationResult = Field(description="Reconciliation agent output")


class EvidenceInput(BaseModel):
    """Evidence details for audit flag creation"""
    matched: bool = Field(description="Whether transaction was matched")
    high_amount: bool = Field(False, description="Whether amount exceeds threshold")
    vendor: str = Field(description="Vendor name")
    anomaly_type: Optional[str] = Field(None, description="Type of anomaly detected")
    contributing_factors: Optional[List[str]] = Field(None, description="List of contributing factors")

# Global list for collecting flags in test mode
_test_mode_flags = []


def get_test_mode_flags():
    """Retrieve flags collected during test mode"""
    return _test_mode_flags.copy()


def clear_test_mode_flags():
    """Clear test mode flags (call at start of each test run)"""
    global _test_mode_flags
    _test_mode_flags = []

@tool("calculate_severity_score")
def calculate_severity_score(transaction: TransactionInput, agent_results: AgentResultsInput) -> dict[str, Any]:
    """
    Calculate severity score based on Data Quality and Reconciliation outputs (rule-based)

    SIMPLIFIED 4-AGENT PIPELINE SCORING:
    - Not matched in reconciliation: +50 points (increased weight - primary indicator)
    - Incomplete data quality: +30 points
    - High transaction amount (>$10k): +20 points (heuristic)

    Args:
        transaction: Transaction data
        agent_results: Results from Data Quality and Reconciliation agents {
            'data_quality': {...},
            'reconciliation': {...}
        }

    Returns:
        {
            'severity_score': int (0-100),
            'level': 'CRITICAL' | 'WARNING' | 'INFO',
            'confidence': float,
            'contributing_factors': list
        }
    """
    # Convert Pydantic models to dicts for internal use
    # Handle both Pydantic models and plain dicts (CrewAI may pass either)
    txn = transaction.model_dump() if hasattr(transaction, 'model_dump') else transaction
    results = agent_results.model_dump() if hasattr(agent_results, 'model_dump') else agent_results

    logger.info(f"Calculating severity for txn {txn.get('txn_id', 'unknown')}")

    try:
        score = 0
        factors = []

        # Factor 1: Reconciliation match (highest weight - now primary indicator)
        if not results.get('reconciliation', {}).get('matched', False):
            score += 50
            factors.append('no_reconciliation_match')

        # Factor 2: Data quality (incomplete records)
        if results.get('data_quality', {}).get('incomplete', False):
            score += 30
            factors.append('incomplete_data')

        # Factor 3: Amount-based heuristic (no ML/anomaly detection)
        amount = txn.get('amount', 0)
        if amount > 10000:
            score += 20
            factors.append('high_amount')

        # Removed: Anomaly detection (not in 4-agent scope)
        # Removed: Context enrichment (email approval, receipts - not in 4-agent scope)

        # Determine severity level (adjusted thresholds for simplified scoring)
        if score >= 70:
            level = SeverityLevel.CRITICAL
        elif score >= 50:
            level = SeverityLevel.WARNING
        else:
            level = SeverityLevel.INFO

        # Confidence based on number of factors (max 3 now instead of 4)
        confidence = min(len(factors) / 3, 1.0)

        result = {
            'severity_score': min(score, 100),
            'level': level.value,
            'confidence': round(confidence, 2),
            'contributing_factors': factors
        }

        logger.info(f"Severity calculated: {level.value} (score: {score}, factors: {len(factors)})")
        return result

    except Exception as e:
        logger.error(f"Severity calculation failed: {e}")
        return {
            'severity_score': 0,
            'level': SeverityLevel.INFO.value,
            'confidence': 0.0,
            'contributing_factors': []
        }


@tool("generate_root_cause_analysis")
def generate_root_cause_analysis(transaction: TransactionInput, agent_results: AgentResultsInput) -> str:
    """
    Generate human-readable explanation for why transaction was flagged

    Uses template for simple cases, LLM for complex cases (confidence <0.7)

    Args:
        transaction: Transaction data
        agent_results: All agent results

    Returns:
        Human-readable explanation string
    """
    # Convert Pydantic models to dicts
    # Handle both Pydantic models and plain dicts (CrewAI may pass either)
    txn = transaction.model_dump() if hasattr(transaction, 'model_dump') else transaction
    results = agent_results.model_dump() if hasattr(agent_results, 'model_dump') else agent_results

    logger.info(f"Generating root cause analysis for {txn['txn_id']}")

    try:
        # Compute contributing factors from available agent results (data_quality + reconciliation)
        # Note: 'escalation' key never exists in agent_results - compute confidence directly
        factors = []
        if not results.get('reconciliation', {}).get('matched', False):
            factors.append('no_reconciliation_match')
        if results.get('data_quality', {}).get('incomplete', False):
            factors.append('incomplete_data')
        if txn.get('amount', 0) > 10000:
            factors.append('high_amount')

        # Use template whenever we have identifiable factors (fast, no LLM)
        if factors:
            explanations = {
                'no_reconciliation_match': f"No matching bank transaction found for ${txn['amount']} to {txn['vendor']}",
                'incomplete_data': f"Transaction {txn['txn_id']} is missing required fields",
                'high_amount': f"Transaction amount ${txn['amount']} exceeds high-value threshold",
                'high_anomaly_score': f"Transaction amount ${txn['amount']} is statistically unusual for this vendor",
                'no_email_approval': "No email approval or authorization found",
                'no_receipt': "Receipt not found in system"
            }

            reasons = [explanations.get(f, f) for f in factors]

            explanation = f"Flagged because: " + "; ".join(reasons) + "."

            logger.info("Used template for explanation")
            return explanation

        # LLM only for truly ambiguous cases (no identifiable factors)
        else:
            logger.info("Using LLM for complex explanation")

            prompt = f"""
Generate a concise 2-sentence explanation for why this transaction was flagged as suspicious:

Transaction:
- ID: {txn['txn_id']}
- Vendor: {txn['vendor']}
- Amount: ${txn['amount']}
- Date: {txn['date']}

Findings:
- Matched to bank: {results.get('reconciliation', {}).get('matched', False)}
- Anomaly score: {results.get('anomaly', {}).get('anomaly_score', 0)}/100
- Email approval: {results.get('context', {}).get('email_approval', False)}
- Receipt found: {results.get('context', {}).get('receipt_found', False)}

Respond with 2 sentences explaining the red flags:
"""

            explanation = call_llm(prompt, agent_name="Escalation")
            return explanation.strip()

    except Exception as e:
        logger.error(f"Root cause generation failed: {e}")
        return f"Transaction flagged due to multiple suspicious indicators."


@tool("batch_classify_with_llm")
def batch_classify_with_llm(transactions_json: str = "[]", agent_results_list_json: str = "[]") -> list[dict[str, str]]:
    """
    Batch classify edge-case transactions using LLM

    Args:
        transactions_json: JSON array string of transactions like '[{"txn_id": "x", "vendor": "AWS", "amount": 100}, ...]'
        agent_results_list_json: JSON array string of agent results like '[{"reconciliation": {"matched": false}, ...}, ...]'

    Returns:
        List of classifications [
            {'txn_id': 'x', 'severity': 'WARNING', 'explanation': '...'},
            ...
        ]
    """
    # Parse JSON strings to lists
    import json
    transactions = json.loads(transactions_json) if transactions_json else []
    agent_results_list = json.loads(agent_results_list_json) if agent_results_list_json else []

    logger.info(f"Batch classifying {len(transactions)} edge cases with LLM")

    try:
        # Build batch prompt
        batch_context = []
        for txn, results in zip(transactions, agent_results_list):
            batch_context.append(f"""
Transaction {txn['txn_id']}:
- Vendor: {txn['vendor']}
- Amount: ${txn['amount']}
- Matched: {results.get('reconciliation', {}).get('matched', False)}
- Anomaly: {results.get('anomaly', {}).get('anomaly_score', 0)}
- Approval: {results.get('context', {}).get('email_approval', False)}
- Receipt: {results.get('context', {}).get('receipt_found', False)}
""")

        prompt = f"""
Classify these {len(transactions)} transactions as CRITICAL, WARNING, or INFO. For each, provide a brief explanation.

{chr(10).join(batch_context)}

Respond with ONLY a JSON array (no markdown):
[
  {{"txn_id": "...", "severity": "CRITICAL|WARNING|INFO", "explanation": "..."}},
  ...
]
"""

        response = call_llm(prompt, model=ANTHROPIC_SMART_MODEL, agent_name="Escalation")

        # Parse JSON response
        try:
            classifications = json.loads(response.strip())
            logger.info(f"LLM classified {len(classifications)} transactions")
            return classifications
        except json.JSONDecodeError:
            logger.error("Failed to parse LLM response, using fallback")
            return [{'txn_id': t['txn_id'], 'severity': 'WARNING', 'explanation': 'Edge case'} for t in transactions]

    except Exception as e:
        logger.error(f"Batch classification failed: {e}")
        return []


@tool("create_audit_flag")
def create_audit_flag(transaction_id: str, audit_run_id: str, severity: str, explanation: str, evidence: EvidenceInput) -> str:
    """
    Create audit flag entry (would write to database in production)

    Args:
        transaction_id: Transaction ID
        audit_run_id: Audit run ID
        severity: Severity level
        explanation: Human-readable explanation
        evidence: Supporting evidence dict

    Returns:
        Flag ID (UUID)
    """
    # Convert Pydantic model to dict
    # Handle both Pydantic models and plain dicts (CrewAI may pass either)
    evidence_dict = evidence.model_dump() if hasattr(evidence, 'model_dump') else evidence

    flag_id = str(uuid.uuid4())

    logger.info(f"Created flag {flag_id} for txn {transaction_id} (severity: {severity})")

    flag_data = {
        'flag_id': flag_id,
        'transaction_id': transaction_id,
        'audit_run_id': audit_run_id,
        'severity_level': severity,
        'confidence_score': 1.0,
        'explanation': explanation,
        'supporting_evidence_links': evidence_dict,
        'created_at': datetime.now().isoformat()
    }

    logger.info(f"Flag created", **flag_data)

    # Persist to Databricks in production
    from src.db.databricks_writer import write_flag
    write_flag(flag_data)

    # Collect flag in test mode for benchmarking
    if os.getenv('TEST_MODE') == 'true':
        global _test_mode_flags
        _test_mode_flags.append({
            'flag_id': flag_id,
            'txn_id': transaction_id,
            'severity': severity,
            'explanation': explanation
        })

    # Write feedback to unification layer (best-effort)
    from src.integrations.unification_client import try_write_feedback
    try_write_feedback(
        transaction_id=transaction_id,
        signal="flagged",
        source="autonomous",
        reason=explanation,
    )

    return flag_id


@tool("check_escalation_rules")
def check_escalation_rules(severity: str, amount: float, vendor: str) -> str:
    """
    Check domain-specific escalation rules and adjust severity

    Args:
        severity: Current severity level
        amount: Transaction amount
        vendor: Vendor name

    Returns:
        Updated severity level
    """
    logger.info(f"Checking escalation rules for {vendor}, ${amount}, severity {severity}")

    try:
        config = load_config()

        # Rule 1: High-value CRITICAL transactions
        if severity == SeverityLevel.CRITICAL.value and amount > 1000:
            logger.info(f"CRITICAL transaction over $1000 - escalating to CFO")
            return severity  # Keep CRITICAL

        # Rule 2: Whitelisted vendors
        whitelisted = config.get('whitelisted_vendors', [])
        if vendor in whitelisted and severity == SeverityLevel.WARNING.value:
            logger.info(f"Vendor {vendor} is whitelisted - downgrading to INFO")
            return SeverityLevel.INFO.value

        # Rule 3: Low-value INFO flags
        if severity == SeverityLevel.INFO.value and amount < 50:
            logger.info(f"Low-value INFO flag - auto-approving")
            return "AUTO_APPROVED"

        return severity

    except Exception as e:
        logger.error(f"Escalation rules check failed: {e}")
        return severity
