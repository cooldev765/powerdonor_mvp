"""
PowerDonor.ai - IRS Charity Data Ingestion Pipeline

Downloads and processes:
- BMF Extract (~1.8M organizations)
- Publication 78 (~1.2M organizations for deductibility verification)

Usage:
    python ingest_irs_data.py --download   # Download fresh data
    python ingest_irs_data.py --load       # Load into database
    python ingest_irs_data.py --all        # Download and load
"""

import os
import sys
import csv
import zipfile
import argparse
import logging
from datetime import datetime
from pathlib import Path
from io import BytesIO
import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

BMF_REGIONS = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl",
    "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me",
    "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh",
    "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr",
    "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
    "wi", "wy"
]

DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "database": os.getenv("PGDATABASE", "powerdonor"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}


class IRSDataIngester:
    def __init__(self, db_config=None):
        self.db_config = db_config or DB_CONFIG
        self.conn = None
        
    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**self.db_config)
        return self.conn
    
    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
    
    def download_bmf_region(self, region: str) -> Path:
        url = f"https://www.irs.gov/pub/irs-soi/eo_{region}.csv"
        filepath = DATA_DIR / f"eo_{region}.csv"
        
        if filepath.exists():
            logger.info(f"BMF {region.upper()} already exists, skipping")
            return filepath
            
        logger.info(f"Downloading BMF {region.upper()}")
        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded BMF {region.upper()}")
            return filepath
        except requests.RequestException as e:
            logger.error(f"Failed to download BMF {region.upper()}: {e}")
            return None
    
    def download_all_bmf(self) -> list:
        downloaded = []
        for region in BMF_REGIONS:
            filepath = self.download_bmf_region(region)
            if filepath:
                downloaded.append(filepath)
        return downloaded
    
    def download_pub78(self) -> Path:
        filepath = DATA_DIR / "pub78.txt"
        url = "https://apps.irs.gov/pub/epostcard/data-download-pub78.zip"
        
        if filepath.exists():
            logger.info("Publication 78 already exists, skipping")
            return filepath
            
        logger.info("Downloading Publication 78")
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                for name in z.namelist():
                    if name.endswith('.txt'):
                        with z.open(name) as f:
                            with open(filepath, 'wb') as out:
                                out.write(f.read())
                        break
            logger.info("Downloaded Publication 78")
            return filepath
        except Exception as e:
            logger.error(f"Failed to download Publication 78: {e}")
            return None
    
    def _parse_bmf_row(self, row: dict) -> tuple:
        try:
            ein = row.get('EIN', '').strip()
            if not ein or len(ein) != 9:
                return None
            
            ruling_date = None
            ruling_str = row.get('RULING', '')
            if ruling_str and len(ruling_str) >= 6:
                try:
                    year = int(ruling_str[:4])
                    month = int(ruling_str[4:6])
                    ruling_date = f"{year}-{month:02d}-01"
                except:
                    pass
            
            def parse_int(val):
                try:
                    return int(val) if val and val.strip() else None
                except:
                    return None
                    
            activity = row.get('ACTIVITY', '') or ''
            
            return (
                ein,
                row.get('NAME', '')[:500],
                row.get('ICO', '')[:500] or None,
                row.get('STREET', '')[:500] or None,
                row.get('CITY', '')[:100] or None,
                row.get('STATE', '')[:2] or None,
                row.get('ZIP', '')[:10] or None,
                'US',
                row.get('NTEE_CD', '')[:10] or None,
                row.get('SUBSECTION', '')[:10] or None,
                row.get('FOUNDATION', '')[:10] or None,
                row.get('AFFILIATION', '')[:10] or None,
                row.get('CLASSIFICATION', '')[:10] or None,
                ruling_date,
                row.get('DEDUCTIBILITY', '')[:10] or None,
                row.get('DEDUCTIBILITY', '') == '1',
                row.get('ORGANIZATION', '')[:100] or None,
                parse_int(row.get('ASSET_AMT')),
                parse_int(row.get('INCOME_AMT')),
                parse_int(row.get('REVENUE_AMT')),
                row.get('FILING_REQ_CD', '')[:10] or None,
                activity[:3] or None,
                activity[3:6] or None,
                activity[6:9] or None,
                row.get('GEN', '')[:20] or None,
                row.get('GROUP', '') == '1',
                row.get('STATUS', '')[:20] or None,
            )
        except Exception as e:
            logger.debug(f"Error parsing row: {e}")
            return None
    
    def _insert_bmf_batch(self, cur, batch: list):
        sql = """
            INSERT INTO charity_base (
                ein, name, care_of_name, street, city, state, zip, country,
                ntee_code, subsection_code, foundation_code, affiliation_code,
                classification_code, ruling_date, deductibility_code, is_deductible,
                organization_type, asset_amount, income_amount, revenue_amount,
                filing_requirement_code, activity_code_1, activity_code_2, activity_code_3,
                group_exemption_number, is_group_return_filer, bmf_status
            ) VALUES %s
            ON CONFLICT (ein) DO UPDATE SET
                name = EXCLUDED.name,
                street = EXCLUDED.street,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip = EXCLUDED.zip,
                ntee_code = EXCLUDED.ntee_code,
                asset_amount = EXCLUDED.asset_amount,
                income_amount = EXCLUDED.income_amount,
                revenue_amount = EXCLUDED.revenue_amount,
                updated_at = CURRENT_TIMESTAMP
        """
        execute_values(cur, sql, batch, page_size=1000)
    
    def load_bmf_file(self, filepath: Path, batch_size: int = 5000) -> int:
        logger.info(f"Loading BMF data from {filepath}")
        conn = self.connect()
        cur = conn.cursor()
        loaded = 0
        batch = []
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    record = self._parse_bmf_row(row)
                    if record:
                        batch.append(record)
                    if len(batch) >= batch_size:
                        self._insert_bmf_batch(cur, batch)
                        loaded += len(batch)
                        batch = []
                        if loaded % 50000 == 0:
                            logger.info(f"  Loaded {loaded:,} records...")
                if batch:
                    self._insert_bmf_batch(cur, batch)
                    loaded += len(batch)
            conn.commit()
            logger.info(f"Loaded {loaded:,} records from {filepath.name}")
            return loaded
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading {filepath}: {e}")
            raise
        finally:
            cur.close()
    
    def load_all_bmf(self) -> int:
        total = 0
        for filepath in sorted(DATA_DIR.glob("eo_*.csv")):
            total += self.load_bmf_file(filepath)
        logger.info(f"Total BMF records loaded: {total:,}")
        return total
    
    def load_pub78(self, filepath: Path = None) -> int:
        filepath = filepath or DATA_DIR / "pub78.txt"
        if not filepath.exists():
            logger.warning("Publication 78 file not found")
            return 0
            
        logger.info(f"Loading Publication 78")
        conn = self.connect()
        cur = conn.cursor()
        loaded = 0
        batch = []
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    parts = line.strip().split('|')
                    if len(parts) >= 3:
                        ein = parts[0].strip().replace('-', '')
                        if len(ein) == 9 and ein.isdigit():
                            batch.append((parts[2].strip() if len(parts) > 2 else None, ein))
                    if len(batch) >= 5000:
                        cur.executemany(
                            "UPDATE charity_base SET pub78_verified=TRUE, deductibility_limitation=%s WHERE ein=%s",
                            batch
                        )
                        loaded += len(batch)
                        batch = []
                if batch:
                    cur.executemany(
                        "UPDATE charity_base SET pub78_verified=TRUE, deductibility_limitation=%s WHERE ein=%s",
                        batch
                    )
                    loaded += len(batch)
            conn.commit()
            logger.info(f"Verified {loaded:,} organizations")
            return loaded
        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading Publication 78: {e}")
            raise
        finally:
            cur.close()
    
    def get_stats(self) -> dict:
        conn = self.connect()
        cur = conn.cursor()
        stats = {}
        
        cur.execute("SELECT COUNT(*) FROM charity_base")
        stats['total_charities'] = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM charity_base WHERE is_deductible = TRUE")
        stats['deductible_charities'] = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM charity_base WHERE pub78_verified = TRUE")
        stats['pub78_verified'] = cur.fetchone()[0]
        
        cur.execute("""
            SELECT state, COUNT(*) FROM charity_base 
            WHERE state IS NOT NULL GROUP BY state ORDER BY COUNT(*) DESC LIMIT 10
        """)
        stats['top_states'] = cur.fetchall()
        
        cur.execute("""
            SELECT LEFT(ntee_code, 1) as category, COUNT(*) FROM charity_base 
            WHERE ntee_code IS NOT NULL GROUP BY LEFT(ntee_code, 1) ORDER BY COUNT(*) DESC
        """)
        stats['by_ntee_category'] = cur.fetchall()
        
        cur.close()
        return stats


def main():
    parser = argparse.ArgumentParser(description='IRS Charity Data Ingestion')
    parser.add_argument('--download', action='store_true', help='Download IRS data')
    parser.add_argument('--load', action='store_true', help='Load data into database')
    parser.add_argument('--all', action='store_true', help='Download and load all data')
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    parser.add_argument('--region', type=str, help='Specific region only')
    args = parser.parse_args()
    
    if not any([args.download, args.load, args.all, args.stats]):
        parser.print_help()
        return
    
    ingester = IRSDataIngester()
    
    try:
        if args.download or args.all:
            if args.region:
                ingester.download_bmf_region(args.region)
            else:
                ingester.download_all_bmf()
                ingester.download_pub78()
        
        if args.load or args.all:
            if args.region:
                filepath = DATA_DIR / f"eo_{args.region}.csv"
                if filepath.exists():
                    ingester.load_bmf_file(filepath)
            else:
                ingester.load_all_bmf()
                ingester.load_pub78()
        
        if args.stats:
            stats = ingester.get_stats()
            print("\n=== Database Statistics ===")
            print(f"Total charities: {stats['total_charities']:,}")
            print(f"Deductible: {stats['deductible_charities']:,}")
            print(f"Pub78 verified: {stats['pub78_verified']:,}")
            print("\nTop 10 states:")
            for state, count in stats['top_states']:
                print(f"  {state}: {count:,}")
    finally:
        ingester.close()


if __name__ == '__main__':
    main()
