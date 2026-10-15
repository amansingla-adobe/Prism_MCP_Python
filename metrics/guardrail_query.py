import duckdb
import pandas as pd

DB_PATH = "db/prism.duckdb"
TABLE_NAME = "aep_guardrail_usage_last_30_days"

# These guardrails don't have real production metrics yet - exclude from
# general results. If explicitly asked about by name, say so honestly
# instead of returning a misleading 0%.
EXCLUDED_GUARDRAILS = ["Edge Segmentation", "Live Inbound Actions"]


def _status_from_pct(pct):
    if pct is None or pd.isna(pct):
        return "unknown"
    elif pct >= 100:
        return "red"
    elif pct >= 80:
        return "yellow"
    return "green"


def _resolve_sql_for_display(query: str, params: list) -> str:
    resolved = query
    for p in params:
        display_val = f"'{p}'" if isinstance(p, str) else str(p)
        resolved = resolved.replace("?", display_val, 1)
    return resolved.strip()


def _guardrail_filter(guardrail_name):
    """guardrail_name: None = all (excluding not-yet-available ones),
    a single name (str), or a list of 2+ names (comparison mode).
    Matching is case-insensitive, since users/LLM may not match exact casing."""
    if guardrail_name is None:
        placeholders = ", ".join(["?"] * len(EXCLUDED_GUARDRAILS))
        return f"AND guardrailName NOT IN ({placeholders})", list(EXCLUDED_GUARDRAILS)
    if isinstance(guardrail_name, (list, tuple)):
        placeholders = ", ".join(["LOWER(?)"] * len(guardrail_name))
        return f"AND LOWER(guardrailName) IN ({placeholders})", list(guardrail_name)
    return "AND LOWER(guardrailName) = LOWER(?)", [guardrail_name]


def _query_latest_snapshot(sandbox, guardrail_name=None):
    filter_clause, filter_params = _guardrail_filter(guardrail_name)
    query = f"""
        WITH latest_rows AS (
            SELECT
                guardrailName, category, guardrailType, currentUsage, threshold, auditDate,
                ROW_NUMBER() OVER (PARTITION BY guardrailName, sandbox ORDER BY auditDate DESC) AS rn
            FROM {TABLE_NAME}
            WHERE source_table = 'aep_guardrail_usage'
              AND sandbox = ?
              AND lastRefresh IS NOT NULL
              {filter_clause}
        )
        SELECT guardrailName, category, guardrailType, currentUsage, threshold, auditDate,
            CASE WHEN threshold IS NULL OR threshold = 0 THEN NULL
                 ELSE ROUND((currentUsage * 100.0) / threshold, 1) END AS pct_of_threshold
        FROM latest_rows WHERE rn = 1
        ORDER BY pct_of_threshold DESC NULLS LAST
    """
    params = [sandbox] + filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_windowed(sandbox, days, guardrail_name=None):
    days = int(days)
    filter_clause, filter_params = _guardrail_filter(guardrail_name)
    query = f"""
        WITH windowed AS (
            SELECT guardrailName, currentUsage, threshold, auditDate,
                CASE WHEN threshold IS NULL OR threshold = 0 THEN NULL
                     ELSE ROUND((currentUsage * 100.0) / threshold, 1) END AS pct_of_threshold
            FROM {TABLE_NAME}
            WHERE source_table = 'aep_guardrail_usage'
              AND sandbox = ?
              AND lastRefresh IS NOT NULL
              AND auditDate >= (SELECT MAX(auditDate) FROM {TABLE_NAME} WHERE sandbox = ?) - INTERVAL '{days} days'
              {filter_clause}
        )
        SELECT guardrailName, COUNT(*) AS total_readings,
            SUM(CASE WHEN pct_of_threshold >= 100 THEN 1 ELSE 0 END) AS breach_readings,
            MAX(pct_of_threshold) AS max_pct,
            MIN(CASE WHEN pct_of_threshold >= 100 THEN auditDate END) AS first_breach_at,
            MAX(CASE WHEN pct_of_threshold >= 100 THEN auditDate END) AS last_breach_at
        FROM windowed GROUP BY guardrailName
        ORDER BY max_pct DESC NULLS LAST
    """
    params = [sandbox, sandbox] + filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_newly_breaching(sandbox, days_ago=30):
    """Adapted from the data team's reviewed query. NOTE: their version uses
    CURRENT_DATE() (real wall-clock date), which assumes live, fresh data.
    Our local snapshot is a static export, so we anchor on the latest
    auditDate actually present in the table instead - same logic, just
    safe against a snapshot that isn't 'live'."""
    days_ago = int(days_ago)
    filter_clause, filter_params = _guardrail_filter(None)
    query = f"""
        WITH latest_current AS (
            SELECT
                guardrailName, category, guardrailType, currentUsage, threshold, auditDate,
                ROW_NUMBER() OVER (PARTITION BY guardrailName ORDER BY auditDate DESC) AS rn
            FROM {TABLE_NAME}
            WHERE sandbox = ?
              AND lastRefresh IS NOT NULL
              AND auditDate >= (SELECT MAX(auditDate) FROM {TABLE_NAME} WHERE sandbox = ?) - INTERVAL '1 day'
              {filter_clause}
        ),
        latest_historical AS (
            SELECT
                guardrailName,
                currentUsage AS usage_days_ago,
                threshold AS threshold_days_ago,
                ROW_NUMBER() OVER (PARTITION BY guardrailName ORDER BY auditDate DESC) AS rn
            FROM {TABLE_NAME}
            WHERE sandbox = ?
              AND lastRefresh IS NOT NULL
              AND auditDate BETWEEN
                  (SELECT MAX(auditDate) FROM {TABLE_NAME} WHERE sandbox = ?) - INTERVAL '{days_ago + 1} day'
                  AND (SELECT MAX(auditDate) FROM {TABLE_NAME} WHERE sandbox = ?) - INTERVAL '{days_ago - 1} day'
        )
        SELECT
            lc.guardrailName, lc.category, lc.guardrailType, lc.currentUsage, lc.threshold, lc.auditDate,
            ROUND(((lc.currentUsage - lc.threshold) * 100.0 / lc.threshold), 1) AS pct_over_threshold,
            COALESCE(lh.usage_days_ago, 0) AS usage_days_ago,
            lc.currentUsage - COALESCE(lh.usage_days_ago, 0) AS usage_increase
        FROM latest_current lc
        LEFT JOIN latest_historical lh
            ON lc.guardrailName = lh.guardrailName AND lh.rn = 1
        WHERE lc.rn = 1
          AND lc.currentUsage > lc.threshold
          AND COALESCE(lh.usage_days_ago, 0) <= COALESCE(lh.threshold_days_ago, lc.threshold)
        ORDER BY pct_over_threshold DESC
    """
    params = [sandbox, sandbox] + filter_params + [sandbox, sandbox]
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def compute(sandbox: str, guardrail_name=None, window_days: int = None,
            sort_by: str = "pct", top_n: int = None, compare_to_days_ago: int = None):
    """
    guardrail_name: None = all guardrails (excluding not-yet-available ones),
        a single name (str), or a list of 2+ names to compare directly.
    window_days: None = latest snapshot, or N = aggregated over N days.
    sort_by: 'pct' or 'breach_count' (only meaningful with window_days).
    top_n: limit the returned list (ignored in single-guardrail mode).
    compare_to_days_ago: if set, answers "what's newly breaching that wasn't
        a problem N days ago" - overrides guardrail_name/window_days.
    """
    if isinstance(guardrail_name, str) and guardrail_name in EXCLUDED_GUARDRAILS:
        return {"sandbox": sandbox, "guardrail_name": guardrail_name, "found": False,
                "message": f"{guardrail_name} is not yet available in production - its guardrail metric hasn't been enabled."}

    if compare_to_days_ago is not None:
        df = _query_newly_breaching(sandbox, days_ago=compare_to_days_ago)
        sql_used = df.attrs.get("sql_query_used", "")
        newly_breaching = [
            {"guardrail_name": r["guardrailName"], "category": r["category"],
             "guardrail_type": r["guardrailType"], "current_usage": int(r["currentUsage"]),
             "threshold": int(r["threshold"]), "pct_over_threshold": float(r["pct_over_threshold"]),
             "usage_days_ago": int(r["usage_days_ago"]), "usage_increase": int(r["usage_increase"])}
            for _, r in df.iterrows()
        ]
        return {"sandbox": sandbox, "compare_to_days_ago": int(compare_to_days_ago),
                "newly_breaching_count": len(newly_breaching),
                "newly_breaching": newly_breaching, "sql_query_used": sql_used}

    df = _query_latest_snapshot(sandbox, guardrail_name) if window_days is None \
        else _query_windowed(sandbox, window_days, guardrail_name)

    sql_used = df.attrs.get("sql_query_used", "")

    if len(df) == 0:
        return {"sandbox": sandbox, "guardrail_name": guardrail_name,
                "window_days": window_days, "found": False, "guardrails": [],
                "sql_query_used": sql_used}

    if isinstance(guardrail_name, str):
        row = df.iloc[0]
        if window_days is None:
            pct = row["pct_of_threshold"]
            return {"sandbox": sandbox, "guardrail_name": row["guardrailName"], "window_days": None,
                    "found": True, "current_usage": int(row["currentUsage"]), "threshold": int(row["threshold"]),
                    "pct_of_threshold": None if pd.isna(pct) else float(pct),
                    "status": _status_from_pct(pct), "as_of": str(row["auditDate"]),
                    "sql_query_used": sql_used}
        total, breach_count = int(row["total_readings"]), int(row["breach_readings"])
        return {"sandbox": sandbox, "guardrail_name": row["guardrailName"], "window_days": window_days,
                "found": True, "total_readings": total, "breach_count": breach_count,
                "breach_pct_of_window": round((breach_count / total) * 100, 1) if total else 0,
                "max_pct_of_threshold": None if pd.isna(row["max_pct"]) else float(row["max_pct"]),
                "first_breach_at": None if pd.isna(row["first_breach_at"]) else str(row["first_breach_at"]),
                "last_breach_at": None if pd.isna(row["last_breach_at"]) else str(row["last_breach_at"]),
                "ever_breached": breach_count > 0, "sql_query_used": sql_used}

    if window_days is None:
        as_of = str(df["auditDate"].iloc[0])
        all_g = [{"guardrail_name": r["guardrailName"],
                  "pct_of_threshold": None if pd.isna(r["pct_of_threshold"]) else float(r["pct_of_threshold"]),
                  "status": _status_from_pct(r["pct_of_threshold"])} for _, r in df.iterrows()]
        red = [g for g in all_g if g["status"] == "red"]
        yellow = [g for g in all_g if g["status"] == "yellow"]
        overall_status = "red" if red else "yellow" if yellow else "green"
        return {"sandbox": sandbox, "guardrail_name": guardrail_name, "window_days": None, "as_of": as_of,
                "overall_status": overall_status, "total_guardrails_checked": len(all_g),
                "red_count": len(red), "yellow_count": len(yellow),
                "guardrails": all_g[:top_n] if top_n else all_g, "sql_query_used": sql_used}

    all_g = []
    for _, r in df.iterrows():
        total, breach_count = int(r["total_readings"]), int(r["breach_readings"])
        all_g.append({"guardrail_name": r["guardrailName"], "total_readings": total, "breach_count": breach_count,
                       "breach_pct_of_window": round((breach_count / total) * 100, 1) if total else 0,
                       "max_pct_of_threshold": None if pd.isna(r["max_pct"]) else float(r["max_pct"]),
                       "ever_breached": breach_count > 0})
    all_g.sort(key=(lambda g: g["breach_count"]) if sort_by == "breach_count"
               else (lambda g: (g["max_pct_of_threshold"] is None, -(g["max_pct_of_threshold"] or 0))), reverse=(sort_by == "breach_count"))
    return {"sandbox": sandbox, "guardrail_name": guardrail_name, "window_days": window_days,
            "guardrails": all_g[:top_n] if top_n else all_g, "sql_query_used": sql_used}