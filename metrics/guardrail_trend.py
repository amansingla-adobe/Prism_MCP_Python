import duckdb
import pandas as pd


DB_PATH = "db/prism.duckdb"
TABLE_NAME = "aep_guardrail_usage_last_30_days"

QUERY_TEMPLATE = """
WITH windowed AS (
    SELECT
        guardrailName, sandbox, currentUsage, threshold, auditDate,
        CASE WHEN threshold IS NULL OR threshold = 0 THEN NULL
             ELSE ROUND((currentUsage * 100.0) / threshold, 1) END AS pct_of_threshold
    FROM {table}
    WHERE source_table = 'aep_guardrail_usage'
      AND sandbox = ?
      AND auditDate >= (SELECT MAX(auditDate) FROM {table} WHERE sandbox = ?) - INTERVAL '{days} days'
)
SELECT
    guardrailName,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN pct_of_threshold >= 100 THEN 1 ELSE 0 END) AS breach_readings,
    SUM(CASE WHEN pct_of_threshold >= 80 AND pct_of_threshold < 100 THEN 1 ELSE 0 END) AS warning_readings,
    MAX(pct_of_threshold) AS max_pct,
    MIN(CASE WHEN pct_of_threshold >= 100 THEN auditDate END) AS first_breach_at,
    MAX(CASE WHEN pct_of_threshold >= 100 THEN auditDate END) AS last_breach_at
FROM windowed
GROUP BY guardrailName
ORDER BY breach_readings DESC, max_pct DESC NULLS LAST
"""


def compute(sandbox: str, days: int = 7):
    query = QUERY_TEMPLATE.format(table=TABLE_NAME, days=days)
    con = duckdb.connect(DB_PATH)
    rows = con.execute(query, [sandbox, sandbox]).fetchdf()
    con.close()

    if len(rows) == 0:
        return {"sandbox": sandbox, "days": days, "guardrails": []}

    guardrails = []
    for _, r in rows.iterrows():
        total = int(r["total_readings"])
        breach_count = int(r["breach_readings"])
        warning_count = int(r["warning_readings"])
        breach_pct = round((breach_count / total) * 100, 1) if total > 0 else 0

        max_pct = r["max_pct"]
        first_breach = r["first_breach_at"]
        last_breach = r["last_breach_at"]

        guardrails.append({
            "guardrail_name": r["guardrailName"],
            "total_readings": total,
            "breach_count": breach_count,
            "breach_pct_of_window": breach_pct,
            "warning_count": warning_count,
            "max_pct_of_threshold": None if pd.isna(max_pct) else float(max_pct),
            "first_breach_at": None if pd.isna(first_breach) else str(first_breach),
            "last_breach_at": None if pd.isna(last_breach) else str(last_breach),
            "ever_breached": breach_count > 0,
        })

    return {"sandbox": sandbox, "days": days, "guardrails": guardrails}


if __name__ == "__main__":
    import json
    print(json.dumps(compute("xlg-prod", days=7), indent=2, default=str))