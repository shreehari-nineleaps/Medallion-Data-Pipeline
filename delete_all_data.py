import psycopg2
import sys
import logging
from pathlib import Path

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent))
from config import DB_CONFIG, LOG_CONFIG

# Set up logging
log_dir = Path(__file__).parent / LOG_CONFIG['log_dir']
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(log_dir / 'delete_all_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_table_counts():
    """Get record counts for all tables before deletion."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        counts = {}

        # Bronze tables
        bronze_tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']
        logger.info("📊 Current data counts:")
        logger.info("-" * 40)

        for table in bronze_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
                count = cursor.fetchone()[0]
                counts[f'bronze.{table}'] = count
                logger.info(f"  bronze.{table:<15}: {count:>8,} records")
            except psycopg2.Error as e:
                logger.warning(f"  ⚠️  Could not count bronze.{table}: {e}")
                counts[f'bronze.{table}'] = 'N/A'

        # Check for silver tables (not just views)
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'silver' AND table_type = 'BASE TABLE'
        """)
        silver_tables = cursor.fetchall()

        if silver_tables:
            logger.info("\n  Silver tables:")
            for (table_name,) in silver_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM silver.{table_name}")
                    count = cursor.fetchone()[0]
                    counts[f'silver.{table_name}'] = count
                    logger.info(f"  silver.{table_name:<15}: {count:>8,} records")
                except psycopg2.Error as e:
                    logger.warning(f"  ⚠️  Could not count silver.{table_name}: {e}")
                    counts[f'silver.{table_name}'] = 'N/A'
        else:
            logger.info("  No silver tables found (only views exist)")

        # Check for gold tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'gold' AND table_type = 'BASE TABLE'
        """)
        gold_tables = cursor.fetchall()

        if gold_tables:
            logger.info("\n  Gold tables:")
            for (table_name,) in gold_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM gold.{table_name}")
                    count = cursor.fetchone()[0]
                    counts[f'gold.{table_name}'] = count
                    logger.info(f"  gold.{table_name:<20}: {count:>8,} records")
                except psycopg2.Error as e:
                    logger.warning(f"  ⚠️  Could not count gold.{table_name}: {e}")
                    counts[f'gold.{table_name}'] = 'N/A'
        else:
            logger.info("  No gold tables found")

        cursor.close()
        conn.close()
        return counts

    except psycopg2.Error as e:
        logger.error(f"❌ Error getting table counts: {e}")
        return {}


def delete_bronze_data():
    """Delete all data from bronze layer tables."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Tables in deletion order (reverse of creation to respect foreign keys)
        tables_to_delete = [
            'supply_orders',
            'inventory',
            'retail_stores',
            'products',
            'warehouses',
            'suppliers'
        ]

        logger.info("🗑️  Deleting bronze layer data...")
        total_deleted = 0

        for table in tables_to_delete:
            try:
                # Get count before deletion
                cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
                count = cursor.fetchone()[0]

                if count > 0:
                    # Delete all records
                    cursor.execute(f"DELETE FROM bronze.{table}")
                    deleted = cursor.rowcount
                    total_deleted += deleted
                    logger.info(f"  ✓ bronze.{table:<15}: {deleted:>8,} records deleted")
                else:
                    logger.info(f"  - bronze.{table:<15}: already empty")

            except psycopg2.Error as e:
                logger.error(f"  ❌ Error deleting from bronze.{table}: {e}")
                conn.rollback()
                return False

        # Commit all deletions
        conn.commit()
        logger.info(f"\n✅ Bronze layer cleanup completed: {total_deleted:,} total records deleted")

        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error during bronze data deletion: {e}")
        return False


def delete_silver_data():
    """Delete all data from silver layer tables (if any exist)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Find all silver tables (not views)
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'silver' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        silver_tables = cursor.fetchall()

        if not silver_tables:
            logger.info("🔍 No silver tables found to delete (only views exist)")
            cursor.close()
            conn.close()
            return True

        logger.info("🗑️  Deleting silver layer data...")
        total_deleted = 0

        for (table_name,) in silver_tables:
            try:
                # Get count before deletion
                cursor.execute(f"SELECT COUNT(*) FROM silver.{table_name}")
                count = cursor.fetchone()[0]

                if count > 0:
                    # Delete all records
                    cursor.execute(f"DELETE FROM silver.{table_name}")
                    deleted = cursor.rowcount
                    total_deleted += deleted
                    logger.info(f"  ✓ silver.{table_name:<15}: {deleted:>8,} records deleted")
                else:
                    logger.info(f"  - silver.{table_name:<15}: already empty")

            except psycopg2.Error as e:
                logger.error(f"  ❌ Error deleting from silver.{table_name}: {e}")
                conn.rollback()
                return False

        # Commit all deletions
        conn.commit()
        logger.info(f"\n✅ Silver layer cleanup completed: {total_deleted:,} total records deleted")

        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error during silver data deletion: {e}")
        return False


def delete_gold_data():
    """Delete all data from gold layer tables."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Find all gold tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'gold' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        gold_tables = cursor.fetchall()

        if not gold_tables:
            logger.info("🔍 No gold tables found to delete")
            cursor.close()
            conn.close()
            return True

        logger.info("🗑️  Deleting gold layer data...")
        total_deleted = 0

        for (table_name,) in gold_tables:
            try:
                # Get count before deletion
                cursor.execute(f"SELECT COUNT(*) FROM gold.{table_name}")
                count = cursor.fetchone()[0]

                if count > 0:
                    # Delete all records
                    cursor.execute(f"DELETE FROM gold.{table_name}")
                    deleted = cursor.rowcount
                    total_deleted += deleted
                    logger.info(f"  ✓ gold.{table_name:<20}: {deleted:>8,} records deleted")
                else:
                    logger.info(f"  - gold.{table_name:<20}: already empty")

            except psycopg2.Error as e:
                logger.error(f"  ❌ Error deleting from gold.{table_name}: {e}")
                conn.rollback()
                return False

        # Commit all deletions
        conn.commit()
        logger.info(f"\n✅ Gold layer cleanup completed: {total_deleted:,} total records deleted")

        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error during gold data deletion: {e}")
        return False


def reset_sequences():
    """Reset auto-increment sequences for primary keys."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        logger.info("🔄 Resetting sequences...")

        # List of tables with auto-increment primary keys
        tables_with_sequences = [
            'suppliers',
            'products',
            'warehouses',
            'inventory',
            'retail_stores',
            'supply_orders'
        ]

        for table in tables_with_sequences:
            try:
                # Check if sequence exists and reset it
                cursor.execute(f"""
                    SELECT pg_get_serial_sequence('bronze.{table}', '{table.rstrip('s')}_id')
                """)
                sequence_name = cursor.fetchone()[0]

                if sequence_name:
                    cursor.execute(f"ALTER SEQUENCE {sequence_name} RESTART WITH 1")
                    logger.info(f"  ✓ Reset sequence for bronze.{table}")

            except psycopg2.Error as e:
                logger.warning(f"  ⚠️  Could not reset sequence for bronze.{table}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error resetting sequences: {e}")
        return False


def verify_deletion():
    """Verify that all data has been deleted."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        logger.info("🔍 Verifying deletion...")

        bronze_tables = ['suppliers', 'products', 'warehouses', 'inventory', 'retail_stores', 'supply_orders']
        all_empty = True

        for table in bronze_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
                count = cursor.fetchone()[0]
                if count > 0:
                    logger.error(f"  ❌ bronze.{table} still has {count} records")
                    all_empty = False
                else:
                    logger.info(f"  ✓ bronze.{table} is empty")
            except psycopg2.Error as e:
                logger.error(f"  ❌ Error checking bronze.{table}: {e}")
                all_empty = False

        # Check silver tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'silver' AND table_type = 'BASE TABLE'
        """)
        silver_tables = cursor.fetchall()

        for (table_name,) in silver_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM silver.{table_name}")
                count = cursor.fetchone()[0]
                if count > 0:
                    logger.error(f"  ❌ silver.{table_name} still has {count} records")
                    all_empty = False
                else:
                    logger.info(f"  ✓ silver.{table_name} is empty")
            except psycopg2.Error as e:
                logger.error(f"  ❌ Error checking silver.{table_name}: {e}")
                all_empty = False

        # Check gold tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'gold' AND table_type = 'BASE TABLE'
        """)
        gold_tables = cursor.fetchall()

        for (table_name,) in gold_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM gold.{table_name}")
                count = cursor.fetchone()[0]
                if count > 0:
                    logger.error(f"  ❌ gold.{table_name} still has {count} records")
                    all_empty = False
                else:
                    logger.info(f"  ✓ gold.{table_name} is empty")
            except psycopg2.Error as e:
                logger.error(f"  ❌ Error checking gold.{table_name}: {e}")
                all_empty = False

        cursor.close()
        conn.close()
        return all_empty

    except psycopg2.Error as e:
        logger.error(f"❌ Error during verification: {e}")
        return False


def main():
    """Main function to delete all bronze, silver, and gold data."""
    logger.info("🚀 Starting Complete Data Deletion (Bronze, Silver, Gold)")
    logger.info("=" * 70)

    # Show current data counts
    initial_counts = get_table_counts()

    # Confirm deletion
    logger.info("\n⚠️  WARNING: This will delete ALL data from bronze, silver, and gold layers!")
    logger.info("This action cannot be undone.")

    try:
        confirm = input("\nDo you want to continue? (yes/no): ").strip().lower()
        if confirm not in ['yes', 'y']:
            logger.info("❌ Deletion cancelled by user")
            return
    except KeyboardInterrupt:
        logger.info("\n❌ Deletion cancelled by user")
        return

    logger.info("\n🗑️  Starting data deletion process...")

    # Delete gold data first (dependencies)
    logger.info("\n1. Deleting gold layer data...")
    if not delete_gold_data():
        logger.error("❌ Failed to delete gold data")
        sys.exit(1)

    # Delete silver data
    logger.info("\n2. Deleting silver layer data...")
    if not delete_silver_data():
        logger.error("❌ Failed to delete silver data")
        sys.exit(1)

    # Delete bronze data
    logger.info("\n3. Deleting bronze layer data...")
    if not delete_bronze_data():
        logger.error("❌ Failed to delete bronze data")
        sys.exit(1)

    # Reset sequences
    logger.info("\n4. Resetting sequences...")
    if not reset_sequences():
        logger.warning("⚠️  Some sequences could not be reset")

    # Verify deletion
    logger.info("\n5. Verifying deletion...")
    if verify_deletion():
        logger.info("✅ All data successfully deleted and verified")
    else:
        logger.error("❌ Verification failed - some data may remain")

    logger.info("\n🎉 Complete Data Deletion Completed!")
    logger.info("=" * 70)
    logger.info("All bronze, silver, and gold layer data has been removed.")
    logger.info("The table structures remain intact for future data loading.")


if __name__ == "__main__":
    main()
