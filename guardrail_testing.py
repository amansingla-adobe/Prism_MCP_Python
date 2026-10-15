import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
TOKEN = os.getenv("DATABRICKS_TOKEN")

TABLE = "dpaas_uccatalog_stg.aoaitgot.aep_guardrail_usage"

with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN) as conn:
    with conn.cursor() as cur:
        print("--- Exact duplicate check (guardrailName, sandbox, auditDate) ---")
        cur.execute(f"""
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT CONCAT(guardrailName, '|', sandbox, '|', auditDate)) AS distinct_combos
            FROM {TABLE}
        """)
        print(cur.fetchone())

        print("\n--- Distinct guardrailName count and values ---")
        cur.execute(f"SELECT DISTINCT guardrailName FROM {TABLE} ORDER BY 1")
        names = [r[0] for r in cur.fetchall()]
        print(f"Count: {len(names)}")
        print(names)

        print("\n--- Distinct sandbox count and values ---")
        cur.execute(f"SELECT DISTINCT sandbox FROM {TABLE} ORDER BY 1")
        sandboxes = [r[0] for r in cur.fetchall()]
        print(f"Count: {len(sandboxes)}")
        print(sandboxes)

        print("\n--- Readings per guardrail+sandbox+day (top 10 by count) ---")
        cur.execute(f"""
            SELECT guardrailName, sandbox, CAST(CAST(auditDate AS TIMESTAMP) AS DATE) AS day, COUNT(*) AS readings
            FROM {TABLE}
            GROUP BY 1, 2, 3
            ORDER BY readings DESC
            LIMIT 10
        """)
        for row in cur.fetchall():
            print(" ", row)

print("\nDone.")    