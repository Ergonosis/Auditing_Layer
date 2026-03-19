"""Main entry point for audit system"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables FIRST, before any other imports
# Find the .env file in the project root
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

from src.orchestrator.orchestrator_agent import AuditOrchestrator
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("ERGONOSIS AUDITING - Data Auditing Agent Ecosystem")
    logger.info("=" * 60)

    try:
        # Validate required env vars in production
        if os.getenv("ENVIRONMENT") == "production":
            REQUIRED = ["ANTHROPIC_API_KEY", "DATABRICKS_HOST", "DATABRICKS_TOKEN",
                        "DATABRICKS_HTTP_PATH", "UNIFICATION_USER_EMAIL"]
            missing = [v for v in REQUIRED if not os.getenv(v)]
            if missing:
                logger.error(f"Missing required production env vars: {missing}")
                raise RuntimeError(f"Missing env vars: {missing}")

        # Create orchestrator
        orchestrator = AuditOrchestrator()

        # Run audit cycle
        results = orchestrator.run_audit_cycle()

        # Print summary
        logger.info("=" * 60)
        logger.info("AUDIT SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Audit Run ID: {results['audit_run_id']}")
        logger.info(f"Status: {results['status']}")
        logger.info(f"Transactions Processed: {results['transaction_count']}")
        logger.info(f"Flags Created: {results['flags_created']}")
        if 'duration_seconds' in results:
            logger.info(f"Duration: {results['duration_seconds']:.1f}s")
        logger.info("=" * 60)

        return results

    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        raise


if __name__ == "__main__":
    main()
