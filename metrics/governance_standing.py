import duckdb

DB_PATH = "db/prism.duckdb"
TABLE_NAME = "aep_guardrail_usage_last_30_days"

# TEMPORARY: in production this comes from the frontend/user request.
# For the POC, change this manually to test different sandboxes.
SANDBOX = "xlg-prod"

QUERY = f"""
WITH latest_audit AS (
    SELECT MAX(auditDate) AS latest_date
    FROM {TABLE_NAME}
    WHERE source_table = 'aep_guardrail_usage'
      AND sandbox = ?
),
current_status AS (
    SELECT
        t.guardrailName,
        t.sandbox,
        t.category,
        t.guardrailType,
        t.currentUsage,
        t.threshold,
        t.auditDate,
        CASE
            WHEN t.threshold IS NULL OR t.threshold = 0 THEN NULL
            ELSE ROUND((t.currentUsage * 100.0) / t.threshold, 1)
        END AS pct_of_threshold
    FROM {TABLE_NAME} t
    JOIN latest_audit la ON t.auditDate = la.latest_date
    WHERE t.source_table = 'aep_guardrail_usage'
      AND t.sandbox = ?
)
SELECT
    guardrailName,
    sandbox,
    category,
    guardrailType,
    currentUsage,
    threshold,
    pct_of_threshold,
    auditDate,
    CASE
        WHEN pct_of_threshold IS NULL THEN 'unknown'
        WHEN pct_of_threshold >= 100 THEN 'red'
        WHEN pct_of_threshold >= 80 THEN 'yellow'
        ELSE 'green'
    END AS status
FROM current_status
ORDER BY pct_of_threshold DESC NULLS LAST
"""


def compute(sandbox: str = SANDBOX):
    con = duckdb.connect(DB_PATH)
    rows = con.execute(QUERY, [sandbox, sandbox]).fetchdf()
    con.close()

    if len(rows) == 0:
        return {
            "sandbox": sandbox,
            "as_of": None,
            "overall_status": "unknown",
            "total_guardrails_checked": 0,
            "red_count": 0,
            "yellow_count": 0,
            "green_count": 0,
            "unknown_count": 0,
            "red_guardrails": [],
            "yellow_guardrails": [],
        }

    as_of = rows["auditDate"].iloc[0]
    red_rows = rows[rows["status"] == "red"]
    yellow_rows = rows[rows["status"] == "yellow"]
    green_rows = rows[rows["status"] == "green"]
    unknown_rows = rows[rows["status"] == "unknown"]

    if len(red_rows) > 0:
        overall_status = "red"
    elif len(yellow_rows) > 0:
        overall_status = "yellow"
    else:
        overall_status = "green"

    return {
        "sandbox": sandbox,
        "as_of": str(as_of),
        "overall_status": overall_status,
        "total_guardrails_checked": len(rows),
        "red_count": len(red_rows),
        "yellow_count": len(yellow_rows),
        "green_count": len(green_rows),
        "unknown_count": len(unknown_rows),
        "red_guardrails": red_rows[["guardrailName", "pct_of_threshold"]].to_dict("records"),
        "yellow_guardrails": yellow_rows[["guardrailName", "pct_of_threshold"]].to_dict("records"),
    }


if __name__ == "__main__":
    import json
    result = compute()
    print(json.dumps(result, indent=2, default=str))