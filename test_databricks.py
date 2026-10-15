import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
TOKEN = os.getenv("DATABRICKS_TOKEN")

missing = [name for name, val in [
    ("DATABRICKS_SERVER_HOSTNAME", HOST),
    ("DATABRICKS_HTTP_PATH", HTTP_PATH),
    ("DATABRICKS_TOKEN", TOKEN),
] if not val]
if missing:
    raise SystemExit(f"Missing .env values: {', '.join(missing)}")

print(f"Connecting to {HOST} ...")

with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN) as conn:
    with conn.cursor() as cur:
        print("\n--- Connection OK. Catalogs visible to this token ---")
        cur.execute("SHOW CATALOGS")
        for row in cur.fetchall():
            print(" -", row[0])

        for candidate_catalog in ("dpaas_uccatalog_stg", "dpaas_uccatalog_prd"):
            print(f"\n--- Checking schema 'aoaitgot' in catalog '{candidate_catalog}' ---")
            try:
                cur.execute(f"SHOW SCHEMAS IN {candidate_catalog} LIKE 'aoaitgot'")
                if not cur.fetchall():
                    print("   (not accessible / not found)")
                    continue

                cur.execute(f"SHOW TABLES IN {candidate_catalog}.aoaitgot LIKE 'aep_guardrail_usage'")
                if not cur.fetchall():
                    print("   Schema found, but 'aep_guardrail_usage' table not in it.")
                    continue

                full_table = f"{candidate_catalog}.aoaitgot.aep_guardrail_usage"
                print(f"   Table found: {full_table}")

                print("   Columns (real live schema - do NOT assume source_table exists, per doc 01):")
                cur.execute(f"DESCRIBE {full_table}")
                for col in cur.fetchall():
                    print(f"     - {col[0]} ({col[1]})")

                cur.execute(f"SELECT COUNT(*) FROM {full_table}")
                print("   Row count:", cur.fetchone()[0])

                print("   Sample rows:")
                cur.execute(f"SELECT * FROM {full_table} LIMIT 3")
                print("    columns:", [d[0] for d in cur.description])
                for row in cur.fetchall():
                    print("    ", row)

            except Exception as e:
                print(f"   Could not inspect {candidate_catalog}: {e}")

        # Verify the auditDate CAST+INTERVAL fix actually works live, and
        # check the real date range (is live row count 2x local just due to
        # more history?). Kept INSIDE the same cursor/connection block -
        # the previous attempt failed because it ran after the `with`
        # blocks had already closed the cursor.
        print("\n--- Verifying CAST(auditDate AS TIMESTAMP) + INTERVAL arithmetic ---")
        table = "dpaas_uccatalog_stg.aoaitgot.aep_guardrail_usage"
        try:
            cur.execute(f"""
                SELECT MIN(CAST(auditDate AS TIMESTAMP)) AS earliest,
                       MAX(CAST(auditDate AS TIMESTAMP)) AS latest,
                       COUNT(DISTINCT CAST(auditDate AS DATE)) AS distinct_days
                FROM {table}
            """)
            print("  Date range (earliest, latest, distinct_days):", cur.fetchone())

            cur.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE CAST(auditDate AS TIMESTAMP) >=
                      (SELECT MAX(CAST(auditDate AS TIMESTAMP)) FROM {table}) - INTERVAL 7 DAYS
            """)
            print("  Row count in last 7 days (via CAST+INTERVAL):", cur.fetchone()[0])
            print("  SUCCESS - the CAST fix works live.")
        except Exception as e:
            print(f"  FAILED: {e}")

print("\nDone.")