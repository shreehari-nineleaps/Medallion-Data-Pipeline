"""
Main ETL orchestration for Medallion Data Pipeline
Supports Bronze -> Silver -> Gold transformations
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent))
from config import LOG_CONFIG
from silver.silver_builder import SilverBuilder

# Set up logging
log_dir = Path(__file__).parent / LOG_CONFIG['log_dir']
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(log_dir / 'etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def build_bronze():
    """Build Bronze layer - Extract and Load raw data."""
    logger.info("🥉 Building Bronze Layer...")

    try:
        # Import and run bronze data loader
        from bronze.data_loader import load_all_data_to_bronze
        success = load_all_data_to_bronze()

        if success:
            logger.info("✅ Bronze layer built successfully")
            return True
        else:
            logger.error("❌ Bronze layer build failed")
            return False

    except Exception as e:
        logger.error(f"❌ Error building Bronze layer: {e}")
        return False


def build_silver():
    """Build Silver layer - Transform and validate data."""
    logger.info("🥈 Building Silver Layer...")

    try:
        silver_builder = SilverBuilder()

        # Step 1: Setup schemas and audit tables
        logger.info("Step 1: Setting up Silver and Audit schemas...")
        if not silver_builder.setup_schemas():
            logger.error("Failed to setup schemas")
            return False

        # Step 2: Light cleaning in SQL (Bronze -> Silver base)
        logger.info("Step 2: Performing light cleaning in SQL...")
        if not silver_builder.create_silver_base_tables():
            logger.error("Failed to create Silver base tables")
            return False

        # Step 3: Deep validation in Python
        logger.info("Step 3: Performing deep validation in Python...")
        if not silver_builder.deep_validation():
            logger.error("Failed to perform deep validation")
            return False

        # Step 4: Data Quality checks
        logger.info("Step 4: Running Data Quality checks...")
        if not silver_builder.run_data_quality_checks():
            logger.error("Failed to run DQ checks")
            return False

        # Log summary
        silver_builder.log_summary()

        logger.info("✅ Silver layer built successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Error building Silver layer: {e}")
        return False


def build_gold():
    """Build Gold layer - Create business metrics and aggregations."""
    logger.info("🥇 Building Gold Layer...")
    logger.info("⚠️  Gold layer not yet implemented")
    return True


def run_full_pipeline():
    """Run the complete ETL pipeline: Bronze -> Silver -> Gold."""
    start_time = datetime.now()
    logger.info("🚀 STARTING FULL MEDALLION ETL PIPELINE")
    logger.info("=" * 60)

    results = {
        'bronze': False,
        'silver': False,
        'gold': False
    }

    # Build Bronze Layer
    results['bronze'] = build_bronze()

    # Build Silver Layer (only if Bronze succeeds)
    if results['bronze']:
        results['silver'] = build_silver()
    else:
        logger.warning("⚠️  Skipping Silver layer due to Bronze failures")

    # Build Gold Layer (only if Silver succeeds)
    if results['silver']:
        results['gold'] = build_gold()
    else:
        logger.warning("⚠️  Skipping Gold layer due to Silver failures")

    # Summary
    end_time = datetime.now()
    duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("📊 ETL PIPELINE SUMMARY")
    logger.info("=" * 60)

    for layer, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"  {layer.upper():<8}: {status}")

    successful_layers = sum(results.values())
    total_layers = len(results)

    logger.info(f"\n📈 Overall: {successful_layers}/{total_layers} layers completed successfully")
    logger.info(f"⏱️  Total duration: {duration}")

    if successful_layers == total_layers:
        logger.info("🎉 Full pipeline completed successfully!")
        return True
    else:
        logger.warning("⚠️  Pipeline completed with some failures")
        return False


def main():
    """Main entry point with command line arguments."""
    import argparse

    parser = argparse.ArgumentParser(description='Medallion Data Pipeline ETL')
    parser.add_argument('--layer', choices=['bronze', 'silver', 'gold', 'all'],
                       default='all', help='Which layer to build')
    parser.add_argument('--force', action='store_true',
                       help='Force rebuild even if layer exists')

    args = parser.parse_args()

    logger.info(f"🎯 Target layer: {args.layer}")
    if args.force:
        logger.info("🔄 Force rebuild enabled")

    success = False

    if args.layer == 'bronze':
        success = build_bronze()
    elif args.layer == 'silver':
        success = build_silver()
    elif args.layer == 'gold':
        success = build_gold()
    elif args.layer == 'all':
        success = run_full_pipeline()

    if success:
        logger.info("✅ ETL process completed successfully")
        sys.exit(0)
    else:
        logger.error("❌ ETL process failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
