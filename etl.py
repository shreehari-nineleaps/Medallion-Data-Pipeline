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
from gold.gold_builder import GoldBuilder
from gold.push import SupabasePusher

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
        return silver_builder.run_full_cleaning_pipeline()

    except Exception as e:
        logger.error(f"❌ Error building Silver layer: {e}")
        return False


def build_gold():
    """Build Gold layer - Create business metrics and aggregations."""
    logger.info("🥇 Building Gold Layer...")

    try:
        gold_builder = GoldBuilder()
        return gold_builder.run_full_gold_pipeline()

    except Exception as e:
        logger.error(f"❌ Error building Gold layer: {e}")
        return False


def push_to_supabase():
    """Push Gold layer data to Supabase."""
    logger.info("🚀 Pushing Gold Layer to Supabase...")

    try:
        pusher = SupabasePusher()
        return pusher.push_all_gold_tables()

    except Exception as e:
        logger.error(f"❌ Error pushing to Supabase: {e}")
        return False


def run_full_pipeline():
    """Run the complete ETL pipeline: Bronze -> Silver -> Gold."""
    start_time = datetime.now()
    logger.info("🚀 STARTING FULL MEDALLION ETL PIPELINE")
    logger.info("=" * 60)

    results = {
        'bronze': False,
        'silver': False,
        'gold': False,
        'supabase': False
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

    # Push to Supabase (only if Gold succeeds)
    if results['gold']:
        results['supabase'] = push_to_supabase()
    else:
        logger.warning("⚠️  Skipping Supabase push due to Gold failures")

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
    parser.add_argument('--layer', choices=['bronze', 'silver', 'gold', 'supabase', 'all'],
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
    elif args.layer == 'supabase':
        success = push_to_supabase()
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
