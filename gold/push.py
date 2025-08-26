"""
Push Gold Layer data to Supabase (into public schema)
"""

import logging
import sys
import os
from pathlib import Path
from typing import Dict, Tuple, Optional
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG

logger = logging.getLogger(__name__)


class SupabasePusher:
    """Handles pushing gold layer data directly to Supabase public schema."""

    def __init__(self):
        self.local_config = DB_CONFIG
        self.supabase_config = {
            'host': os.getenv('SUPABASE_HOST'),
            'database': os.getenv('SUPABASE_DB_NAME'),
            'user': os.getenv('SUPABASE_USER'),
            'password': os.getenv('SUPABASE_PASSWORD'),
            'port': int(os.getenv('SUPABASE_PORT', '5432'))
        }

        self._validate_supabase_config()

        self.connection_pool = None
        self.pool_initialized = False

        self.gold_tables = [
            'monthly_sales_performance',
            'inventory_health_metrics',
            'supply_chain_dashboard',
            'table_metadata'
        ]

    def _validate_supabase_config(self):
        required_vars = ['SUPABASE_HOST', 'SUPABASE_DB_NAME', 'SUPABASE_USER', 'SUPABASE_PASSWORD']
        missing = [v for v in required_vars if not os.getenv(v)]
        if missing:
            raise ValueError(f"Missing env vars: {', '.join(missing)}")

    def get_local_connection(self):
        return psycopg2.connect(**self.local_config)

    def get_supabase_connection(self):
        if not self.pool_initialized:
            self.connection_pool = ThreadedConnectionPool(1, 10, **self.supabase_config)
            self.pool_initialized = True
        conn = self.connection_pool.getconn()
        conn.autocommit = False
        return conn

    def release_supabase_connection(self, conn):
        """Return a connection back to pool."""
        if self.pool_initialized and self.connection_pool:
            self.connection_pool.putconn(conn)

    def get_table_schema(self, table_name: str) -> Optional[str]:
        conn = self.get_local_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    column_name, data_type, is_nullable, column_default,
                    character_maximum_length, numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = 'gold'
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            columns = cursor.fetchall()
            if not columns:
                return None

            create_statement = f"CREATE TABLE IF NOT EXISTS public.{table_name} (\n"
            defs = []
            for col_name, data_type, is_nullable, default, max_len, precision, scale in columns:
                col_def = f"    {col_name} {data_type}"
                if data_type in ('character varying', 'varchar') and max_len:
                    col_def += f"({max_len})"
                elif data_type == 'numeric' and precision and scale:
                    col_def += f"({precision},{scale})"
                elif data_type == 'ARRAY':
                    col_def = f"    {col_name} text[]"
                if is_nullable == 'NO':
                    col_def += " NOT NULL"
                if default and not str(default).startswith('nextval'):
                    col_def += f" DEFAULT {default}"
                defs.append(col_def)
            create_statement += ",\n".join(defs) + "\n);"
            return create_statement
        finally:
            conn.close()

    def create_table_in_supabase(self, table_name: str) -> bool:
        conn = self.get_supabase_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS public.{table_name} CASCADE")
            cursor.execute(self.get_table_schema(table_name))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error creating {table_name} in Supabase: {e}")
            conn.rollback()
            return False
        finally:
            self.release_supabase_connection(conn)

    def push_table_data(self, table_name: str, batch_size: int = 10000) -> bool:
        local_conn = self.get_local_connection()
        supabase_conn = self.get_supabase_connection()
        try:
            local_cursor = local_conn.cursor(cursor_factory=RealDictCursor)
            local_cursor.execute(f"SELECT * FROM gold.{table_name}")
            cols = [desc[0] for desc in local_cursor.description]
            col_list = ', '.join(cols)

            sup_cursor = supabase_conn.cursor()
            sup_cursor.execute(f"TRUNCATE TABLE public.{table_name}")

            total_rows, batch_count = 0, 0
            while True:
                batch = local_cursor.fetchmany(batch_size)
                if not batch:
                    break
                batch_count += 1
                batch_data = [tuple(row[c] for c in cols) for row in batch]
                execute_values(
                    sup_cursor,
                    f"INSERT INTO public.{table_name} ({col_list}) VALUES %s",
                    batch_data, page_size=5000
                )
                total_rows += len(batch)
                if batch_count % 3 == 0:
                    supabase_conn.commit()
            supabase_conn.commit()
            logger.info(f"Pushed {total_rows:,} rows to {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error pushing data for {table_name}: {e}")
            supabase_conn.rollback()
            return False
        finally:
            local_conn.close()
            self.release_supabase_connection(supabase_conn)

    def verify_table_data(self, table_name: str) -> Tuple[bool, Dict[str, int]]:
        local_conn = self.get_local_connection()
        sup_conn = self.get_supabase_connection()
        try:
            lc = local_conn.cursor()
            lc.execute(f"SELECT COUNT(*) FROM gold.{table_name}")
            local_count = lc.fetchone()[0]

            sc = sup_conn.cursor()
            sc.execute(f"SELECT COUNT(*) FROM public.{table_name}")
            sup_count = sc.fetchone()[0]

            success = (local_count == sup_count and local_count > 0)
            return success, {"local": local_count, "supabase": sup_count}
        finally:
            local_conn.close()
            self.release_supabase_connection(sup_conn)

    def push_single_table(self, table_name: str) -> bool:
        if not self.create_table_in_supabase(table_name):
            return False
        if not self.push_table_data(table_name):
            return False
        success, counts = self.verify_table_data(table_name)
        if not success:
            logger.error(f"Verification failed for {table_name}: {counts}")
            return False
        return True

    def push_all_gold_tables(self, parallel: bool = False) -> bool:
        results = {}
        if parallel:
            with ThreadPoolExecutor(max_workers=3) as executor:
                futs = {executor.submit(self.push_single_table, t): t for t in self.gold_tables}
                for fut in as_completed(futs):
                    t = futs[fut]
                    results[t] = fut.result()
        else:
            for t in self.gold_tables:
                results[t] = self.push_single_table(t)
        success_count = sum(1 for ok in results.values() if ok)
        logger.info(f"Pushed {success_count}/{len(self.gold_tables)} tables")
        return success_count == len(self.gold_tables)

    def close_pool(self):
        if self.pool_initialized and self.connection_pool:
            self.connection_pool.closeall()


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', type=str)
    parser.add_argument('--parallel', action='store_true')
    args = parser.parse_args()

    pusher = SupabasePusher()
    try:
        if args.table:
            ok = pusher.push_single_table(args.table)
            sys.exit(0 if ok else 1)
        else:
            ok = pusher.push_all_gold_tables(parallel=args.parallel)
            sys.exit(0 if ok else 1)
    finally:
        pusher.close_pool()


if __name__ == "__main__":
    main()
