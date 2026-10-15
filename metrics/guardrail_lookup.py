import duckdb

DB_PATH = "db/prism.duckdb"
TABLE_NAME = "aep_guardrail_usage_last_30_days"

QUERY = f"""
WITH latest_row AS (
    SELECT
        guardrailName, sandbox, category, guardrailType,
        currentUsage, threshold, auditDate,
        ROW_NUMBER() OVER (PARTITION BY guardrailName, sandbox ORDER BY auditDate DESC) AS rn
    FROM {TABLE_NAME}
    WHERE source_table = 'aep_guardrail_usage'
      AND sandbox = ?
      AND guardrailName = ?
)
SELECT
    guardrailName, sandbox, category, guardrailType,
    currentUsage, threshold, auditDate,
    CASE WHEN threshold IS NULL OR threshold = 0 THEN NULL
         ELSE ROUND((currentUsage * 100.0) / threshold, 1) END AS pct_of_threshold
FROM latest_row
WHERE rn = 1
"""


def compute(sandbox: str, guardrail_name: str):
    con = duckdb.connect(DB_PATH)
    rows = con.execute(QUERY, [sandbox, guardrail_name]).fetchdf()
    con.close()

    if len(rows) == 0:
        return {
            "sandbox": sandbox,
            "guardrail_name": guardrail_name,
            "found": False,
            "message": f"No data found for guardrail '{guardrail_name}' in sandbox '{sandbox}'.",
        }

    row = rows.iloc[0]
    pct = row["pct_of_threshold"]
    if pct is None:
        status = "unknown"
    elif pct >= 100:
        status = "red"
    elif pct >= 80:
        status = "yellow"
    else:
        status = "green"

    return {
        "sandbox": sandbox,
        "guardrail_name": row["guardrailName"],
        "found": True,
        "category": row["category"],
        "guardrail_type": row["guardrailType"],
        "current_usage": int(row["currentUsage"]),
        "threshold": int(row["threshold"]),
        "pct_of_threshold": float(pct) if pct is not None else None,
        "status": status,
        "as_of": str(row["auditDate"]),
    }


if __name__ == "__main__":
    import json
    print(json.dumps(compute("xlg-prod", "Streaming Ingestion"), indent=2, default=str))