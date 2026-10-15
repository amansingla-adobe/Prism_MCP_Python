import duckdb
import os

RAW_DIR = "data/raw"
DB_PATH = "db/prism.duckdb"

def main():
    csv_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]

    if not csv_files:
        print("No CSV files found in data/raw/. Add your data file there first.")
        return

    con = duckdb.connect(DB_PATH)

    for filename in csv_files:
        path = os.path.join(RAW_DIR, filename)
        table_name = os.path.splitext(filename)[0]
        print(f"\nLoading '{filename}' into table '{table_name}'...")

        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{path}', HEADER=TRUE)
        """)

        schema = con.execute(f"DESCRIBE {table_name}").fetchall()
        print(f"Columns in '{table_name}':")
        for col in schema:
            print(f"  - {col[0]} ({col[1]})")

        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Row count: {row_count}")

        print("Sample rows:")
        sample = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
        print(sample)

    con.close()
    print(f"\nDatabase built successfully at {DB_PATH}")

if __name__ == "__main__":
    main()