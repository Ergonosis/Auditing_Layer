"""Reconciliation tools for cross-source transaction matching"""

from crewai.tools import tool
import pandas as pd
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta
from src.tools.databricks_client import query_gold_tables
from src.utils.logging import get_logger
from src.utils.sql_utils import sanitize_sql_value, validate_numeric, validate_identifier
from difflib import SequenceMatcher
import numpy as np

logger = get_logger(__name__)

_PRELOADED_DATA: dict = {}


def _get_transactions_df(transactions_json: str = "[]") -> pd.DataFrame:
    """Three-tier data fetch: cache → JSON arg → Databricks fallback."""
    if _PRELOADED_DATA.get("populated"):
        logger.debug(f"Cache hit: {len(_PRELOADED_DATA['transactions'])} rows")
        return _PRELOADED_DATA["transactions"].copy()
    if transactions_json and transactions_json not in ("[]", "{}"):
        try:
            import json as _json
            records = _json.loads(transactions_json)
            if records:
                logger.debug(f"Using transactions_json arg ({len(records)} records)")
                return pd.DataFrame(records)
        except Exception as exc:
            logger.warning(f"Failed to parse transactions_json; falling back to Databricks: {exc}")
    logger.info("Cache not populated — querying Databricks")
    return query_gold_tables("SELECT * FROM ergonosis.unification.transactions")


@tool("cross_source_matcher")
def cross_source_matcher(transactions_json: str = "[]", source_1: str = "credit_card", source_2: str = "bank", date_range_json: str = '["2025-01-01", "2025-12-31"]') -> dict[str, Any]:
    """
    Match transactions from two sources based on amount, date, and vendor

    Matching criteria:
    - Amount: exact match or ±5% (for currency conversion)
    - Date: ±3 days window
    - Vendor: exact match or KG-resolved entity match

    Args:
        transactions_json: Pass '[]' to load from preloaded cache (default).
        source_1: First source name (e.g., 'credit_card')
        source_2: Second source name (e.g., 'bank')
        date_range_json: JSON array of [start_date, end_date] as ISO strings like '["2025-01-01", "2025-01-31"]'

    Returns:
        {
            'matched_pairs': [
                {'source_1_id': 'cc_123', 'source_2_id': 'bank_456', 'confidence': 0.95, 'match_reason': 'exact_amount_date'},
                ...
            ],
            'unmatched_source_1': ['cc_789', ...],
            'unmatched_source_2': ['bank_999', ...],
            'match_rate': 0.88
        }
    """
    try:
        # Parse JSON array to tuple
        import json
        date_range = json.loads(date_range_json) if date_range_json else ["2025-01-01", "2025-12-31"]
        start_date, end_date = date_range

        logger.info(f"Matching {source_1} vs {source_2} for date range {date_range}")

        all_df = _get_transactions_df(transactions_json)

        # Filter by source and date range
        all_df['date'] = pd.to_datetime(all_df['date'])
        date_mask = (all_df['date'] >= pd.to_datetime(start_date)) & (all_df['date'] <= pd.to_datetime(end_date))
        df1 = all_df[date_mask & (all_df['source'] == source_1)].copy()
        df2 = all_df[date_mask & (all_df['source'] == source_2)].copy()

        if df1.empty or df2.empty:
            logger.warning(f"No data found for {source_1} or {source_2}")
            return {
                'matched_pairs': [],
                'unmatched_source_1': [],
                'unmatched_source_2': [],
                'match_rate': 0.0
            }

        matched_pairs = []
        matched_ids_1 = set()
        matched_ids_2 = set()

        # Match on amount + date + vendor
        for idx1, row1 in df1.iterrows():
            for idx2, row2 in df2.iterrows():
                if row2['txn_id'] in matched_ids_2:
                    continue  # Already matched

                # Amount matching (±5%)
                amount_match = abs(row1['amount'] - row2['amount']) / row1['amount'] <= 0.05

                # Date matching (±3 days)
                date_diff = abs((row1['date'] - row2['date']).days)
                date_match = date_diff <= 3

                # Vendor matching (exact or KG entity)
                vendor_match = (
                    row1['vendor'] == row2['vendor'] or
                    (row1.get('vendor_id') and row1['vendor_id'] == row2.get('vendor_id'))
                )

                if amount_match and date_match and vendor_match:
                    confidence = 0.95 if date_diff == 0 and row1['amount'] == row2['amount'] else 0.85

                    matched_pairs.append({
                        'source_1_id': row1['txn_id'],
                        'source_2_id': row2['txn_id'],
                        'confidence': confidence,
                        'match_reason': f'amount_match={amount_match}, date_diff={date_diff}',
                        'amount': row1['amount'],
                        'vendor': row1['vendor']
                    })

                    matched_ids_1.add(row1['txn_id'])
                    matched_ids_2.add(row2['txn_id'])
                    break  # Move to next row1

        unmatched_1 = [txn for txn in df1['txn_id'].tolist() if txn not in matched_ids_1]
        unmatched_2 = [txn for txn in df2['txn_id'].tolist() if txn not in matched_ids_2]

        match_rate = len(matched_pairs) / len(df1) if len(df1) > 0 else 0

        result = {
            'matched_pairs': matched_pairs[:100],  # Limit output size
            'unmatched_source_1': unmatched_1[:50],
            'unmatched_source_2': unmatched_2[:50],
            'match_rate': round(match_rate, 3),
            'total_matched': len(matched_pairs),
            'total_unmatched_1': len(unmatched_1),
            'total_unmatched_2': len(unmatched_2)
        }

        logger.info(f"Matching complete: {len(matched_pairs)} matches, match rate {match_rate:.1%}")
        return result

    except Exception as e:
        logger.error(f"Cross-source matching failed: {e}")
        raise


@tool("entity_resolver_kg")
def entity_resolver_kg(vendor_name: str) -> dict[str, Any]:
    """
    Resolve vendor entity using Knowledge Graph (Delta Lake tables)

    Args:
        vendor_name: Raw vendor name (e.g., "AMZN MKTP US*1A2B3C4D5")

    Returns:
        {
            'canonical_entity_id': 'amazon_marketplace',
            'canonical_name': 'Amazon Marketplace',
            'aliases': ['AMZN MKTP', 'Amazon.com', 'AMAZON MKTPLACE'],
            'confidence': 0.95
        }
    """
    logger.info(f"Resolving entity for vendor: {vendor_name}")

    try:
        # Query KG entities table (Delta Lake)
        safe_vendor = sanitize_sql_value(vendor_name)
        result = query_gold_tables(f"""
            SELECT entity_id, canonical_name, aliases
            FROM kg_entities
            WHERE '{safe_vendor}' IN (canonical_name, aliases)
               OR aliases LIKE '%{safe_vendor}%'
            LIMIT 1
        """)

        if not result.empty:
            row = result.iloc[0]
            return {
                'canonical_entity_id': row['entity_id'],
                'canonical_name': row['canonical_name'],
                'aliases': row.get('aliases', []),
                'confidence': 0.95
            }
        else:
            # No exact match - return unknown
            logger.warning(f"No KG entity found for {vendor_name}")
            return {
                'canonical_entity_id': f'unknown_{vendor_name.replace(" ", "_").lower()}',
                'canonical_name': vendor_name,
                'aliases': [],
                'confidence': 0.0
            }

    except Exception as e:
        logger.error(f"Entity resolution failed: {e}")
        return {
            'canonical_entity_id': f'unknown_{vendor_name}',
            'canonical_name': vendor_name,
            'aliases': [],
            'confidence': 0.0
        }


@tool("fuzzy_vendor_matcher")
def fuzzy_vendor_matcher(vendor_a: str, vendor_b: str) -> float:
    """
    Calculate fuzzy match similarity between two vendor names

    Uses Levenshtein distance (SequenceMatcher)

    Args:
        vendor_a: First vendor name
        vendor_b: Second vendor name

    Returns:
        Similarity score 0-1 (1 = exact match)
    """
    try:
        # Normalize (lowercase, strip whitespace)
        a = vendor_a.lower().strip()
        b = vendor_b.lower().strip()

        # Calculate similarity
        similarity = SequenceMatcher(None, a, b).ratio()

        logger.info(f"Fuzzy match '{vendor_a}' vs '{vendor_b}': {similarity:.2f}")
        return round(similarity, 3)

    except Exception as e:
        logger.error(f"Fuzzy matching failed: {e}")
        return 0.0


@tool("receipt_transaction_matcher")
def receipt_transaction_matcher(receipt_data_json: str = "{}", transactions_table: str = "gold.transactions") -> dict[str, Any]:
    """
    Match OCR-extracted receipt to credit card transaction

    Args:
        receipt_data_json: JSON string of receipt data like '{"vendor": "AWS", "amount": 100.0, "date": "2025-02-01"}'
            Keys: vendor (str), amount (float), date (str in ISO format)
        transactions_table: Name of transactions table

    Returns:
        {
            'matched_transaction_id': str or None,
            'confidence': float,
            'amount_delta': float,
            'date_delta_days': int
        }
    """
    # Parse JSON string to dict
    import json
    receipt_data = json.loads(receipt_data_json) if receipt_data_json else {}

    logger.info(f"Matching receipt: {receipt_data}")

    try:
        vendor = receipt_data.get('vendor', '')
        amount = receipt_data.get('amount', 0.0)
        receipt_date = pd.to_datetime(receipt_data.get('date', '2025-01-01'))

        # Query transactions ±7 days from receipt date
        start_date = receipt_date - timedelta(days=7)
        end_date = receipt_date + timedelta(days=7)

        safe_table = validate_identifier(transactions_table)
        safe_sd = sanitize_sql_value(str(start_date.date()))
        safe_ed = sanitize_sql_value(str(end_date.date()))
        safe_amount = validate_numeric(amount)

        df = query_gold_tables(f"""
            SELECT txn_id, vendor, amount, date
            FROM {safe_table}
            WHERE date BETWEEN '{safe_sd}' AND '{safe_ed}'
              AND amount BETWEEN {safe_amount * 0.95} AND {safe_amount * 1.05}
        """)

        if df.empty:
            logger.warning("No matching transactions found for receipt")
            return {
                'matched_transaction_id': None,
                'confidence': 0.0,
                'amount_delta': None,
                'date_delta_days': None
            }

        # Find best match
        df['date'] = pd.to_datetime(df['date'])
        df['amount_delta'] = abs(df['amount'] - amount)
        df['date_delta'] = abs((df['date'] - receipt_date).dt.days)
        df['vendor_similarity'] = df['vendor'].apply(lambda v: fuzzy_vendor_matcher.func(v, vendor))

        # Score = weighted combination
        df['score'] = (
            (1 - df['amount_delta'] / amount) * 0.4 +
            (1 - df['date_delta'] / 7) * 0.3 +
            df['vendor_similarity'] * 0.3
        )

        best_match = df.loc[df['score'].idxmax()]

        result = {
            'matched_transaction_id': best_match['txn_id'],
            'confidence': round(best_match['score'], 3),
            'amount_delta': round(best_match['amount_delta'], 2),
            'date_delta_days': int(best_match['date_delta'])
        }

        logger.info(f"Receipt matched to {result['matched_transaction_id']} (confidence: {result['confidence']})")
        return result

    except Exception as e:
        logger.error(f"Receipt matching failed: {e}")
        return {
            'matched_transaction_id': None,
            'confidence': 0.0,
            'amount_delta': None,
            'date_delta_days': None
        }


@tool("find_orphan_transactions")
def find_orphan_transactions(transactions_json: str = "[]", all_sources_json: str = '["credit_card","bank"]') -> dict[str, Any]:
    """
    Find transactions that appear in only one source (SUSPICIOUS)

    Args:
        transactions_json: Pass '[]' to load from preloaded cache (default).
        all_sources_json: JSON array of source names like '["credit_card", "bank"]'

    Returns:
        {
            'orphan_count': int,
            'orphans': [
                {'txn_id': 'cc_123', 'source': 'credit_card', 'amount': 500, 'vendor': 'Unknown Inc'},
                ...
            ]
        }
    """
    # Coerce all_sources_json
    if isinstance(all_sources_json, str):
        import json as _json
        all_sources = _json.loads(all_sources_json) if all_sources_json.startswith('[') else [all_sources_json]
    else:
        all_sources = all_sources_json

    logger.info(f"Finding orphan transactions across sources: {all_sources}")

    try:
        df = _get_transactions_df(transactions_json)
        combined = df[df['source'].isin(all_sources)].copy() if 'source' in df.columns else df.copy()

        if combined.empty:
            return {'orphan_count': 0, 'orphans': []}

        # Group by (amount, vendor, date) to find transactions in only one source
        combined['key'] = combined['amount'].astype(str) + '_' + combined['vendor'] + '_' + combined['date'].astype(str)

        orphans_df = combined[combined.groupby('key')['source'].transform('nunique') == 1]

        orphans = orphans_df.to_dict('records')[:50]  # Limit to 50

        result = {
            'orphan_count': len(orphans_df),
            'orphans': orphans
        }

        logger.info(f"Found {result['orphan_count']} orphan transactions")
        return result

    except Exception as e:
        logger.error(f"Orphan detection failed: {e}")
        return {'orphan_count': 0, 'orphans': []}
