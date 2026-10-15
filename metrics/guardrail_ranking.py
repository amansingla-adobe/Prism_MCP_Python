import duckdb

DB_PATH = "db/prism.duckdb"
TABLE_NAME = "aep_guardrail_usage_last_30_days"

QUERY = f"""
WITH latest_rows AS (
    SELECT
        guardrailName, sandbox, category, guardrailType,
        currentUsage, threshold, auditDate,
        ROW_NUMBER() OVER (PARTITION BY guardrailName, sandbox ORDER BY auditDate DESC) AS rn
    FROM {TABLE_NAME}
    WHERE source_table = 'aep_guardrail_usage'
      AND sandbox = ?
),
current_status AS (
    SELECT
        guardrailName, category, guardrailType, currentUsage, threshold, auditDate,
        CASE WHEN threshold IS NULL OR threshold = 0 THEN NULL
             ELSE ROUND((currentUsage * 100.0) / threshold, 1) END AS pct_of_threshold
    FROM latest_rows
    WHERE rn = 1
)
SELECT * FROM current_status
ORDER BY pct_of_threshold DESC NULLS LAST
"""


def compute(sandbox: str, top_n: int = 5):
    con = duckdb.connect(DB_PATH)
    rows = con.execute(QUERY, [sandbox]).fetchdf()
    con.close()

    if len(rows) == 0:
        return {"sandbox": sandbox, "as_of": None, "guardrails": []}

    as_of = rows["auditDate"].iloc[0]
    top_rows = rows.head(top_n)

    guardrails = []
    for _, r in top_rows.iterrows():
        pct = r["pct_of_threshold"]
        if pct is None:
            status = "unknown"
        elif pct >= 100:
            status = "red"
        elif pct >= 80:
            status = "yellow"
        else:
            status = "green"
        guardrails.append({
            "guardrail_name": r["guardrailName"],
            "pct_of_threshold": float(pct) if pct is not None else None,
            "status": status,
        })

    return {"sandbox": sandbox, "as_of": str(as_of), "guardrails": guardrails}


if __name__ == "__main__":
    import json
    print(json.dumps(compute("xlg-prod", top_n=5), indent=2, default=str))