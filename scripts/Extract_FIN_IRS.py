import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    'host': os.environ.get("PGHOST", "localhost"),
    'database': os.environ.get("PGDATABASE", "powerdonor"),
    'user': os.environ.get("PGUSER", "postgres"),
    'password': os.environ["PGPASSWORD"],
    'port': int(os.environ.get("PGPORT", "5432")),
}

BASE_PATH = os.environ.get("IRS_FORMS_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "IRS Forms 990"))


def create_tables(conn):
    cur = conn.cursor()

    # Drop existing table if exists
    cur.execute("DROP TABLE IF EXISTS irs_990")

    # Create unified 990 table
    cur.execute("""
        CREATE TABLE irs_990 (
            id SERIAL PRIMARY KEY,
            ein VARCHAR(9) NOT NULL,
            tax_year INTEGER,
            tax_period INTEGER,
            form_type VARCHAR(10),
            total_revenue BIGINT,
            total_expenses BIGINT,
            total_assets BIGINT,
            total_liabilities BIGINT,
            net_assets BIGINT,
            contributions BIGINT,
            program_revenue BIGINT,
            investment_income BIGINT,
            officer_compensation BIGINT,
            other_salaries BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ein, tax_period, form_type)
        )
    """)

    cur.execute("CREATE INDEX idx_990_ein ON irs_990(ein)")
    cur.execute("CREATE INDEX idx_990_revenue ON irs_990(total_revenue DESC NULLS LAST)")
    conn.commit()
    print("Table created")


def load_990(conn, filepath, year, form_type):
    print(f"Loading {filepath}...")

    df = pd.read_csv(filepath, encoding='latin-1', low_memory=False)

    # Normalize column names to lowercase
    df.columns = df.columns.str.lower().str.strip()

    # Remove BOM if present
    df.columns = [col.replace('ï»¿', '') for col in df.columns]

    # Map columns based on form type
    if form_type == '990':
        col_map = {
            'ein': 'ein',
            'tax_pd': 'tax_period',
            'totrevenue': 'total_revenue',
            'totfuncexpns': 'total_expenses',
            'totassetsend': 'total_assets',
            'totliabend': 'total_liabilities',
            'totnetassetend': 'net_assets',
            'totcntrbgfts': 'contributions',
            'totprgmrevnue': 'program_revenue',
            'invstmntinc': 'investment_income',
            'compnsatncurrofcr': 'officer_compensation',
            'othrsalwages': 'other_salaries'
        }
    elif form_type == '990EZ':
        col_map = {
            'ein': 'ein',
            'taxpd': 'tax_period',
            'totrevnue': 'total_revenue',
            'totexpns': 'total_expenses',
            'totassetsend': 'total_assets',
            'totliabend': 'total_liabilities',
            'totnetassetsend': 'net_assets',
            'totcntrbs': 'contributions',
            'prgmservrev': 'program_revenue',
            'othrinvstinc': 'investment_income'
        }
    else:  # 990PF
        col_map = {
            'ein': 'ein',
            'tax_prd': 'tax_period',
            'totrcptperbks': 'total_revenue',
            'totexpnspbks': 'total_expenses',
            'totassetsend': 'total_assets',
            'totliabend': 'total_liabilities',
            'totnetassets': 'net_assets',
            'grscontrgifts': 'contributions',
            'intrstrvnue': 'investment_income',
            'compofficers': 'officer_compensation'
        }

    # Select and rename columns that exist
    available_cols = {k: v for k, v in col_map.items() if k in df.columns}

    # Debug: show what columns were found
    print(f"  Found columns: {list(available_cols.keys())}")

    if 'ein' not in available_cols:
        print(f"  ERROR: 'ein' not found. Available columns: {list(df.columns[:10])}")
        return

    df_subset = df[list(available_cols.keys())].rename(columns=available_cols)

    # Add form type and tax year
    df_subset['form_type'] = form_type
    df_subset['tax_year'] = int(f"20{year}")

    # If tax_period not found, create from year
    if 'tax_period' not in df_subset.columns:
        df_subset['tax_period'] = int(f"20{year}12")

    # Clean EIN - ensure string, remove .0, pad with zeros, truncate to 9
    df_subset['ein'] = df_subset['ein'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(9).str[:9]

    # Convert numeric columns
    numeric_cols = ['total_revenue', 'total_expenses', 'total_assets', 'total_liabilities',
                    'net_assets', 'contributions', 'program_revenue', 'investment_income',
                    'officer_compensation', 'other_salaries']

    for col in numeric_cols:
        if col in df_subset.columns:
            df_subset[col] = pd.to_numeric(df_subset[col], errors='coerce')

    # Fill missing columns with None
    for col in numeric_cols:
        if col not in df_subset.columns:
            df_subset[col] = None

    # Also ensure tax_period is in columns
    if 'tax_period' not in df_subset.columns:
        df_subset['tax_period'] = None

    # Insert into database
    cur = conn.cursor()

    columns = ['ein', 'tax_period', 'tax_year', 'form_type', 'total_revenue', 'total_expenses',
               'total_assets', 'total_liabilities', 'net_assets', 'contributions',
               'program_revenue', 'investment_income', 'officer_compensation', 'other_salaries']

    values = df_subset[columns].values.tolist()

    # Clean NaN values
    clean_values = []
    for row in values:
        clean_row = [None if pd.isna(v) else v for v in row]
        clean_values.append(clean_row)

    insert_sql = f"""
        INSERT INTO irs_990 ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (ein, tax_period, form_type) DO UPDATE SET
            total_revenue = EXCLUDED.total_revenue,
            total_expenses = EXCLUDED.total_expenses,
            total_assets = EXCLUDED.total_assets,
            total_liabilities = EXCLUDED.total_liabilities,
            net_assets = EXCLUDED.net_assets,
            contributions = EXCLUDED.contributions,
            program_revenue = EXCLUDED.program_revenue,
            investment_income = EXCLUDED.investment_income,
            officer_compensation = EXCLUDED.officer_compensation,
            other_salaries = EXCLUDED.other_salaries
    """

    execute_values(cur, insert_sql, clean_values, page_size=1000)
    conn.commit()

    print(f"  Loaded {len(clean_values)} records")


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    # Create table
    create_tables(conn)

    # Load all files
    files = [
        ('22', '990', f"{BASE_PATH}\\22eoextract990.csv"),
        ('22', '990EZ', f"{BASE_PATH}\\22eoextractez.csv"),
        ('22', '990PF', f"{BASE_PATH}\\22eoextract990pf.csv"),
        ('23', '990', f"{BASE_PATH}\\23eoextract990.csv"),
        ('23', '990EZ', f"{BASE_PATH}\\23eoextractez.csv"),
        ('23', '990PF', f"{BASE_PATH}\\23eoextract990pf.csv"),
        ('24', '990', f"{BASE_PATH}\\24eoextract990.csv"),
        ('24', '990EZ', f"{BASE_PATH}\\24eoextract990EZ.csv"),
        ('24', '990PF', f"{BASE_PATH}\\24eoextract990pf.csv"),
    ]

    for year, form_type, filepath in files:
        load_990(conn, filepath, year, form_type)

    # Show summary
    cur = conn.cursor()
    cur.execute(
        "SELECT form_type, tax_year, COUNT(*), SUM(total_revenue) FROM irs_990 GROUP BY form_type, tax_year ORDER BY tax_year, form_type")
    print("\nSummary:")
    print(f"{'Form':<10} {'Year':<6} {'Count':<10} {'Total Revenue':<20}")
    for row in cur.fetchall():
        rev = f"${row[3]:,.0f}" if row[3] else "N/A"
        print(f"{row[0]:<10} {row[1]:<6} {row[2]:<10} {rev}")

    # Total unique EINs
    cur.execute("SELECT COUNT(DISTINCT ein) FROM irs_990")
    print(f"\nTotal unique organizations: {cur.fetchone()[0]:,}")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()