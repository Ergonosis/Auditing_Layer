"""Data Quality validation tools"""

from crewai.tools import tool
import pandas as pd
from typing import Dict, Any, List
from src.tools.databricks_client import query_gold_tables
from src.utils.config_loader import load_config, get_domain_config
from src.utils.logging import get_logger
from src.utils.sql_utils import validate_identifier
import json
import os

logger = get_logger(__name__)

_PRELOADED_DATA: dict = {}


def _get_transactions_df(transactions_json: str = "[]") -> pd.DataFrame:
    """Three-tier data fetch: cache → JSON arg → Databricks fallback."""
    if _PRELOADED_DATA.get("populated"):
        logger.debug(f"Cache hit: {len(_PRELOADED_DATA['transactions'])} rows")
        return _PRELOADED_DATA["transactions"].copy()
    if transactions_json and transactions_json not in ("[]", "{}"):
        try:
            records = json.loads(transactions_json)
            if records:
                logger.debug(f"Using transactions_json arg ({len(records)} records)")
                return pd.DataFrame(records)
        except Exception as exc:
            logger.warning("Failed to parse transactions_json; falling back to Databricks", error=str(exc))
    logger.info("Cache not populated — querying Databricks")
    return query_gold_tables("SELECT * FROM ergonosis.unification.transactions")


@tool("check_data_completeness")
def check_data_completeness(transactions_json: str = "[]") -> dict[str, Any]:
    """
    Validate data completeness - check if required fields are populated

    Args:
        transactions_json: Pass '[]' to load from preloaded cache (default).

    Returns:
        Dictionary with completeness metrics:
        {
            'total_records': int,
            'missing_vendor': int,
            'missing_amount': int,
            'missing_date': int,
            'missing_source': int,
            'completeness_score': float  # 0-1
        }
    """
    logger.info("Checking data completeness")

    try:
        df = _get_transactions_df(transactions_json)

        if df.empty:
            logger.warning("No transaction data found")
            return {
                'total_records': 0,
                'missing_vendor': 0,
                'missing_amount': 0,
                'missing_date': 0,
                'missing_source': 0,
                'completeness_score': 0.0
            }

        total_records = len(df)
        required_fields = ['vendor', 'amount', 'date', 'source']

        missing_counts = {}
        for field in required_fields:
            if field in df.columns:
                missing_counts[f'missing_{field}'] = int(df[field].isnull().sum())
            else:
                missing_counts[f'missing_{field}'] = total_records

        # Calculate completeness score
        total_cells = total_records * len(required_fields)
        missing_cells = sum(missing_counts.values())
        completeness_score = 1 - (missing_cells / total_cells) if total_cells > 0 else 0

        result = {
            'total_records': total_records,
            **missing_counts,
            'completeness_score': round(completeness_score, 3)
        }

        logger.info(f"Completeness check complete", **result)
        return result

    except Exception as e:
        logger.error(f"Completeness check failed: {e}")
        raise


@tool("validate_schema_conformity")
def validate_schema_conformity(transactions_json: str = "[]", expected_schema_json: str = "{}") -> list[dict[str, str]]:
    """
    Validate schema conformity - check data types and structure

    Args:
        transactions_json: Pass '[]' to load from preloaded cache (default).
        expected_schema_json: JSON string of expected schema like '{"amount": "float", "vendor": "str", "date": "datetime"}'

    Returns:
        List of validation errors (empty if all valid)
        [
            {'field': 'amount', 'error': 'Invalid type', 'details': '...'},
            ...
        ]
    """
    # Parse JSON string to dict
    expected_schema = json.loads(expected_schema_json) if expected_schema_json else {}
    logger.info("Validating schema")

    try:
        df = _get_transactions_df(transactions_json)

        if df.empty:
            return []

        errors = []

        for field, expected_type in expected_schema.items():
            if field not in df.columns:
                errors.append({
                    'field': field,
                    'error': 'Missing field',
                    'details': f'Field {field} not found in table'
                })
                continue

            # Check data type
            actual_type = str(df[field].dtype)

            type_map = {
                'float': ['float64', 'float32'],
                'int': ['int64', 'int32'],
                'str': ['object'],
                'datetime': ['datetime64[ns]', 'datetime64']
            }

            if expected_type in type_map:
                if actual_type not in type_map[expected_type]:
                    errors.append({
                        'field': field,
                        'error': 'Type mismatch',
                        'details': f'Expected {expected_type}, got {actual_type}'
                    })

        logger.info(f"Schema validation complete: {len(errors)} errors found")
        return errors

    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        raise


@tool("detect_duplicate_records")
def detect_duplicate_records(transactions_json: str = "[]", key_fields_json: str = '["txn_id"]') -> dict[str, Any]:
    """
    Detect duplicate records based on key fields

    Args:
        transactions_json: Pass '[]' to load from preloaded cache (default).
        key_fields_json: JSON array of fields to check for duplicates
                   e.g., '["txn_id"]' or '["vendor", "amount", "date"]'

    Returns:
        Dictionary with duplicate info:
        {
            'duplicate_count': int,
            'duplicate_groups': [
                {'key': 'value', 'count': 3, 'ids': ['id1', 'id2', 'id3']},
                ...
            ]
        }
    """
    # Coerce key_fields_json to list
    if isinstance(key_fields_json, str):
        key_fields = json.loads(key_fields_json) if key_fields_json.startswith('[') else [key_fields_json]
    else:
        key_fields = key_fields_json

    logger.info(f"Detecting duplicates on fields {key_fields}")

    try:
        df = _get_transactions_df(transactions_json)

        if df.empty or not all(field in df.columns for field in key_fields):
            return {'duplicate_count': 0, 'duplicate_groups': []}

        # Find duplicates
        duplicates = df[df.duplicated(subset=key_fields, keep=False)]

        if duplicates.empty:
            logger.info("No duplicates found")
            return {'duplicate_count': 0, 'duplicate_groups': []}

        # Group duplicates
        duplicate_groups = []
        for key_values, group in duplicates.groupby(key_fields):
            duplicate_groups.append({
                'key': dict(zip(key_fields, key_values if isinstance(key_values, tuple) else [key_values])),
                'count': len(group),
                'ids': group.get('txn_id', group.index).tolist()[:5]  # Limit to 5 IDs
            })

        result = {
            'duplicate_count': len(duplicates),
            'duplicate_groups': duplicate_groups[:10]  # Limit to 10 groups
        }

        logger.info(f"Found {result['duplicate_count']} duplicates in {len(duplicate_groups)} groups")
        return result

    except Exception as e:
        logger.error(f"Duplicate detection failed: {e}")
        raise


@tool("infer_domain_freshness")
def infer_domain_freshness(transaction_pattern_json: str = "{}") -> dict[str, Any]:
    """
    Infer domain-specific freshness requirements

    Args:
        transaction_pattern_json: JSON string of transaction characteristics like:
            '{"frequency": "daily", "vendor_type": "supplier", "avg_amount": 100.0, "domain": "inventory"}'
            Keys: frequency, vendor_type, avg_amount, domain (optional)

    Returns:
        Domain configuration:
        {
            'domain': 'inventory_management' | 'senior_living' | 'business_operations' | 'default',
            'max_age_hours': int,
            'confidence': float,
            'source': 'manual_config' | 'inferred'
        }
    """
    # Parse JSON string to dict
    transaction_pattern = json.loads(transaction_pattern_json) if transaction_pattern_json else {}
    logger.info(f"Inferring domain freshness for pattern: {transaction_pattern}")

    try:
        # Load configuration
        config = load_config()

        # Check if domain is manually specified
        if 'domain' in transaction_pattern:
            domain = transaction_pattern['domain']
            domain_config = get_domain_config(config, domain)

            if domain_config:
                logger.info(f"Using manual domain config for {domain}")
                return {
                    'domain': domain,
                    'max_age_hours': domain_config.get('max_age_hours', 48),
                    'confidence': 1.0,
                    'source': 'manual_config',
                    'critical_amount_threshold': domain_config.get('critical_amount_threshold', 1000)
                }

        # No manual config — return hardcoded default.
        # (LLM fallback removed: without a real transaction pattern the LLM
        # has no signal and always returns the same generic response.)
        logger.info("No domain config found, using default")
        return {
            'domain': 'default',
            'max_age_hours': 48,
            'confidence': 0.5,
            'source': 'default_config'
        }

    except Exception as e:
        logger.error(f"Domain inference failed: {e}")
        # Fallback to default
        return {
            'domain': 'default',
            'max_age_hours': 48,
            'confidence': 0.0,
            'source': 'error_fallback'
        }


@tool("check_data_quality_gates")
def check_data_quality_gates(quality_metrics_json: str = "{}", thresholds_json: str = '{"completeness_threshold": 0.9}') -> bool:
    """
    Check if data quality meets minimum thresholds

    Args:
        quality_metrics_json: JSON string of quality metrics like '{"completeness_score": 0.95}'
        thresholds_json: JSON string of thresholds like '{"completeness_threshold": 0.90}'

    Returns:
        True if quality passes gates, False if audit should halt
    """
    # Parse JSON strings to dicts
    quality_metrics = json.loads(quality_metrics_json) if quality_metrics_json else {}
    thresholds = json.loads(thresholds_json) if thresholds_json else {"completeness_threshold": 0.9}
    logger.info("Checking data quality gates")

    try:
        completeness_threshold = thresholds.get('completeness_threshold', 0.90)
        completeness_score = quality_metrics.get('completeness_score', 0)

        if completeness_score < completeness_threshold:
            logger.warning(
                f"Data quality gate FAILED: completeness {completeness_score} < {completeness_threshold}"
            )
            return False

        logger.info(f"Data quality gate PASSED: completeness {completeness_score} >= {completeness_threshold}")
        return True

    except Exception as e:
        logger.error(f"Quality gate check failed: {e}")
        return False
