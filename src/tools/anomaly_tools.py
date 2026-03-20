"""Anomaly detection tools using ML and statistical methods"""

from crewai.tools import tool
import hashlib
import os
import pandas as pd
import numpy as np
import pickle
from pathlib import Path
from typing import Dict, Any, List
from src.tools.databricks_client import query_gold_tables
from src.utils.logging import get_logger
from src.utils.sql_utils import sanitize_sql_value, validate_numeric
from sklearn.ensemble import IsolationForest

logger = get_logger(__name__)

# Model is loaded lazily on first use to avoid import-time RCE risk from
# untrusted pickle artifacts.  See _get_isolation_forest_model().
_isolation_forest_model = None

_MODEL_PATH = Path(os.path.abspath("src/ml/isolation_forest_model.pkl"))
_PROJECT_ROOT = Path(os.path.abspath("."))


def _get_isolation_forest_model() -> IsolationForest:
    """Lazy-load the Isolation Forest model with integrity verification.

    If the env var ISOLATION_FOREST_MODEL_SHA256 is set, the file's SHA-256
    digest is verified before loading.  The model is cached in-process after
    the first successful load.
    """
    global _isolation_forest_model
    if _isolation_forest_model is not None:
        return _isolation_forest_model

    # Enforce that the resolved path stays within the project root.
    try:
        _MODEL_PATH.resolve().relative_to(_PROJECT_ROOT.resolve())
    except ValueError:
        raise ValueError(f"Model path {_MODEL_PATH} is outside the project root — refusing to load")

    if _MODEL_PATH.exists():
        expected_hash = os.getenv("ISOLATION_FOREST_MODEL_SHA256")
        if expected_hash:
            actual_hash = hashlib.sha256(_MODEL_PATH.read_bytes()).hexdigest()
            if actual_hash != expected_hash:
                raise RuntimeError(
                    f"Model artifact hash mismatch — refusing to load pickle. "
                    f"Expected {expected_hash}, got {actual_hash}"
                )
        with open(_MODEL_PATH, "rb") as f:
            _isolation_forest_model = pickle.load(f)
        logger.info("Loaded pre-trained Isolation Forest model")
    else:
        _isolation_forest_model = IsolationForest(contamination=0.05, random_state=42)
        logger.warning("No pre-trained model found, using default Isolation Forest")

    return _isolation_forest_model


@tool("run_isolation_forest")
def run_isolation_forest(transactions_json: str = "[]") -> dict[str, Any]:
    """
    Run Isolation Forest anomaly detection on transaction features

    Features used:
    - amount (log-scaled)
    - vendor_id (encoded)
    - day_of_week, day_of_month
    - time_since_last_transaction_from_vendor

    Args:
        transactions_json: JSON array string of transaction dicts like '[{"txn_id": "x", "amount": 100, "vendor_id": "v1", "date": "2025-01-01"}, ...]'
            Required keys: txn_id, amount, vendor_id, date

    Returns:
        {
            'anomaly_scores': [
                {'txn_id': 'x', 'score': -0.8, 'is_anomaly': True},
                ...
            ],
            'anomaly_count': int
        }
    """
    # Parse JSON string to list
    import json
    transactions = json.loads(transactions_json) if transactions_json else []

    logger.info(f"Running Isolation Forest on {len(transactions)} transactions")

    try:
        df = pd.DataFrame(transactions)

        if df.empty:
            return {'anomaly_scores': [], 'anomaly_count': 0}

        # Feature engineering
        df['date'] = pd.to_datetime(df['date'])
        df['log_amount'] = np.log1p(df['amount'])
        df['day_of_week'] = df['date'].dt.dayofweek
        df['day_of_month'] = df['date'].dt.day

        # Vendor encoding (simple hash for now)
        df['vendor_encoded'] = df['vendor_id'].fillna('unknown').apply(lambda x: hash(x) % 1000)

        # Features for model
        features = ['log_amount', 'vendor_encoded', 'day_of_week', 'day_of_month']
        X = df[features].fillna(0)

        # Predict anomaly scores
        model = _get_isolation_forest_model()
        if hasattr(model, 'decision_function'):
            scores = model.decision_function(X)
        else:
            # Train model if not fitted
            model.fit(X)
            scores = model.decision_function(X)

        df['anomaly_score'] = scores
        df['is_anomaly'] = scores < -0.5  # Threshold for anomaly

        # Prepare output
        anomaly_scores = df[['txn_id', 'anomaly_score', 'is_anomaly']].to_dict('records')

        result = {
            'anomaly_scores': anomaly_scores[:100],  # Limit output
            'anomaly_count': int(df['is_anomaly'].sum())
        }

        logger.info(f"Isolation Forest complete: {result['anomaly_count']} anomalies detected")
        return result

    except Exception as e:
        logger.error(f"Isolation Forest failed: {e}")
        return {'anomaly_scores': [], 'anomaly_count': 0}


@tool("check_vendor_spending_profile")
def check_vendor_spending_profile(vendor_id: str, amount: float) -> dict[str, Any]:
    """
    Check if transaction amount is typical for vendor

    Args:
        vendor_id: Canonical vendor ID
        amount: Transaction amount

    Returns:
        {
            'is_outlier': bool,
            'z_score': float,
            'vendor_mean': float,
            'vendor_std': float,
            'explanation': str
        }
    """
    logger.info(f"Checking spending profile for {vendor_id}, amount ${amount}")

    try:
        # Query vendor profile from cached stats
        safe_vendor_id = sanitize_sql_value(vendor_id)
        safe_amount = validate_numeric(amount)
        profile = query_gold_tables(f"""
            SELECT mean_amount, std_dev, frequency
            FROM gold.vendor_profiles
            WHERE vendor_id = '{safe_vendor_id}'
        """)

        if profile.empty:
            logger.warning(f"No profile found for {vendor_id}")
            return {
                'is_outlier': False,
                'z_score': 0.0,
                'vendor_mean': amount,
                'vendor_std': 0.0,
                'explanation': 'No historical data for vendor'
            }

        mean = profile['mean_amount'].iloc[0]
        std = profile['std_dev'].iloc[0]

        # Calculate z-score
        z_score = (safe_amount - mean) / std if std > 0 else 0

        is_outlier = abs(z_score) > 3.0

        explanation = f"Amount ${safe_amount:.2f} is {z_score:.1f}σ from mean ${mean:.2f}"

        result = {
            'is_outlier': is_outlier,
            'z_score': round(z_score, 2),
            'vendor_mean': round(mean, 2),
            'vendor_std': round(std, 2),
            'explanation': explanation
        }

        logger.info(f"Vendor profile check: {'OUTLIER' if is_outlier else 'NORMAL'}")
        return result

    except Exception as e:
        logger.error(f"Vendor profile check failed: {e}")
        return {
            'is_outlier': False,
            'z_score': 0.0,
            'vendor_mean': 0.0,
            'vendor_std': 0.0,
            'explanation': f'Error: {e}'
        }


@tool("detect_amount_outliers")
def detect_amount_outliers(transactions_json: str = "[]") -> dict[str, Any]:
    """
    Simple statistical outlier detection using z-scores

    Args:
        transactions_json: JSON array string of transaction dicts like '[{"txn_id": "x", "amount": 100}, ...]'

    Returns:
        {
            'outliers': [{'txn_id': 'x', 'amount': 5000, 'z_score': 4.2}, ...],
            'outlier_count': int
        }
    """
    # Parse JSON string to list
    import json
    transactions = json.loads(transactions_json) if transactions_json else []

    logger.info(f"Detecting amount outliers in {len(transactions)} transactions")

    try:
        df = pd.DataFrame(transactions)

        if df.empty or 'amount' not in df.columns:
            return {'outliers': [], 'outlier_count': 0}

        # Calculate z-scores
        mean = df['amount'].mean()
        std = df['amount'].std()

        df['z_score'] = (df['amount'] - mean) / std if std > 0 else 0
        df['is_outlier'] = abs(df['z_score']) > 2.0  # 2 sigma threshold

        outliers = df[df['is_outlier']][['txn_id', 'amount', 'z_score']].to_dict('records')

        result = {
            'outliers': outliers[:50],
            'outlier_count': len(outliers)
        }

        logger.info(f"Found {result['outlier_count']} amount outliers")
        return result

    except Exception as e:
        logger.error(f"Amount outlier detection failed: {e}")
        return {'outliers': [], 'outlier_count': 0}


@tool("time_series_deviation_check")
def time_series_deviation_check(recurring_transactions_json: str = "[]") -> dict[str, Any]:
    """
    Check for deviations in recurring transactions (simplified version)

    Args:
        recurring_transactions_json: JSON array string of recurring transactions like '[{"txn_id": "x", "amount": 100, "date": "2025-01-01"}, ...]'

    Returns:
        {
            'deviations': [{'txn_id': 'x', 'expected': 100, 'actual': 150, 'deviation_pct': 50}, ...],
            'deviation_count': int
        }
    """
    # Parse JSON string to list
    import json
    recurring_transactions = json.loads(recurring_transactions_json) if recurring_transactions_json else []

    logger.info(f"Checking time series deviations for {len(recurring_transactions)} transactions")

    try:
        df = pd.DataFrame(recurring_transactions)

        if df.empty or len(df) < 3:
            return {'deviations': [], 'deviation_count': 0}

        # Calculate rolling mean
        df = df.sort_values('date')
        df['rolling_mean'] = df['amount'].rolling(window=3, min_periods=1).mean()
        df['deviation_pct'] = abs((df['amount'] - df['rolling_mean']) / df['rolling_mean']) * 100

        # Flag deviations >20%
        df['is_deviation'] = df['deviation_pct'] > 20

        deviations = df[df['is_deviation']][['txn_id', 'amount', 'rolling_mean', 'deviation_pct']].to_dict('records')

        result = {
            'deviations': deviations[:20],
            'deviation_count': len(deviations)
        }

        logger.info(f"Found {result['deviation_count']} time series deviations")
        return result

    except Exception as e:
        logger.error(f"Time series check failed: {e}")
        return {'deviations': [], 'deviation_count': 0}


@tool("batch_anomaly_scorer")
def batch_anomaly_scorer(transactions_json: str = "[]") -> dict[str, Any]:
    """
    Combine all anomaly signals into single score (0-100)

    Args:
        transactions_json: JSON array string of transactions with anomaly data like '[{"txn_id": "x", "is_anomaly_if": true, ...}, ...]'

    Returns:
        {
            'scored_transactions': [
                {'txn_id': 'x', 'anomaly_score': 85, 'reasons': ['isolation_forest', 'vendor_outlier']},
                ...
            ],
            'high_risk_count': int  # score >70
        }
    """
    # Parse JSON string to list
    import json
    transactions = json.loads(transactions_json) if transactions_json else []

    logger.info(f"Scoring {len(transactions)} transactions")

    try:
        scored = []

        for txn in transactions:
            score = 0
            reasons = []

            # Isolation Forest signal
            if txn.get('is_anomaly_if'):
                score += 30
                reasons.append('isolation_forest')

            # Vendor outlier signal
            if txn.get('is_vendor_outlier'):
                score += 30
                reasons.append('vendor_outlier')

            # Amount outlier signal
            if txn.get('is_amount_outlier'):
                score += 20
                reasons.append('amount_outlier')

            # Time series deviation
            if txn.get('is_time_series_deviation'):
                score += 20
                reasons.append('time_series_deviation')

            scored.append({
                'txn_id': txn['txn_id'],
                'anomaly_score': min(score, 100),
                'reasons': reasons
            })

        high_risk = [s for s in scored if s['anomaly_score'] > 70]

        result = {
            'scored_transactions': scored[:100],
            'high_risk_count': len(high_risk)
        }

        logger.info(f"Scoring complete: {result['high_risk_count']} high-risk transactions")
        return result

    except Exception as e:
        logger.error(f"Batch scoring failed: {e}")
        return {'scored_transactions': [], 'high_risk_count': 0}
