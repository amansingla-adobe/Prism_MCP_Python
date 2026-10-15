import duckdb
import pandas as pd
from metrics.segment_job_query import _query_jobs_by_date, _query_latest_available_date

DB_PATH = "db/prism.duckdb"
JOB_TABLE = "aep_segment_job_last_45_days"
AUDIENCE_TABLE = "aep_segment_job_audiences_last_45_days"
SEGMENT_TABLE = "aep_segment_b2b_last_30_days"

DEFAULT_THRESHOLD_PCT = 30.0


def _resolve_sql_for_display(query: str, params: list) -> str:
    resolved = query
    for p in params:
        display_val = f"'{p}'" if isinstance(p, str) else str(p)
        resolved = resolved.replace("?", display_val, 1)
    return resolved.strip()


def _query_audiences_for_jobs(sandbox, job_ids: list):
    """Pulls every audience row for the given job IDs, joined to the parent
    job for totalProfiles/status context. qualifiedProfiles/pctOfTotalProfiles
    come straight from the source table - confirmed these already match the
    30%-rule math exactly, so no re-derivation needed."""
    if not job_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(job_ids))
    query = f"""
        SELECT a.jobId, a.audienceId, a.qualifiedProfiles, a.realizedCount, a.exitedCount,
               a.pctOfTotalProfiles, j.totalProfiles, j.status AS job_status,
               CAST(j.creationTime AS DATE) AS job_date
        FROM {AUDIENCE_TABLE} a
        JOIN {JOB_TABLE} j ON a.jobId = j.jobId AND a.sandboxName = j.sandboxName
        WHERE a.sandboxName = ? AND a.jobId IN ({placeholders})
        ORDER BY a.jobId, a.pctOfTotalProfiles DESC NULLS LAST
    """
    params = [sandbox] + job_ids
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_segment_info(sandbox, segment_ids: list, on_date):
    """Segment metadata (name/status/definition/type) for the EXACT calendar
    day requested - per the agreed rule, never falls back to 'latest
    available' silently. Dedupes the tag fan-out (confirmed up to 7 rows per
    segment+day) into one row per segment with a tagIDs list, using
    ANY_VALUE() for fields that are identical across tag-duplicate rows."""
    if not segment_ids:
        return pd.DataFrame()
    placeholders = ", ".join(["?"] * len(segment_ids))
    query = f"""
        SELECT segmentId,
               ANY_VALUE(segmentName) AS segmentName,
               ANY_VALUE(lifecycleState) AS lifecycleState,
               ANY_VALUE(audienceDefinitionPqlText) AS audienceDefinitionPqlText,
               ANY_VALUE(segmentType) AS segmentType,
               ANY_VALUE(evaluationType) AS evaluationType,
               LIST(DISTINCT tagID) AS tag_ids
        FROM {SEGMENT_TABLE}
        WHERE sandboxName = ? AND auditDate = ? AND segmentId IN ({placeholders})
        GROUP BY segmentId
    """
    params = [sandbox, str(on_date)] + segment_ids
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _resolve_segment_name(sandbox, segment_name):
    """Case-insensitive name -> segmentId(s). Never silently collapses -
    if the name matches more than one distinct ID, all are returned (same
    'never assume one is THE match' policy used for job_date -> multiple jobs."""
    query = f"""
        SELECT DISTINCT segmentId FROM {SEGMENT_TABLE}
        WHERE sandboxName = ? AND LOWER(segmentName) = LOWER(?)
    """
    con = duckdb.connect(DB_PATH)
    ids = con.execute(query, [sandbox, segment_name]).fetchdf()["segmentId"].tolist()
    con.close()
    return ids


def _query_audience_history(sandbox, audience_id, days):
    """One audience's qualification history across every job in the last N
    days - for 'ever crossed threshold' / 'consistently highest %' questions."""
    days = int(days)
    query = f"""
        SELECT a.jobId, a.qualifiedProfiles, a.pctOfTotalProfiles,
               CAST(j.creationTime AS DATE) AS job_date
        FROM {AUDIENCE_TABLE} a
        JOIN {JOB_TABLE} j ON a.jobId = j.jobId AND a.sandboxName = j.sandboxName
        WHERE a.sandboxName = ? AND a.audienceId = ?
          AND j.creationTime >= (SELECT MAX(creationTime) FROM {JOB_TABLE} WHERE sandboxName = ?) - INTERVAL '{days} days'
        ORDER BY j.creationTime
    """
    params = [sandbox, audience_id, sandbox]
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_ranked_audiences(sandbox, days, top_n=None):
    """Across ALL audiences in the window: avg/max pct and how often each
    was empty - for 'which audience is consistently highest' / 'most
    frequently empty' style questions."""
    days = int(days)
    query = f"""
        SELECT a.audienceId,
               COUNT(*) AS appearances,
               AVG(a.pctOfTotalProfiles) AS avg_pct,
               MAX(a.pctOfTotalProfiles) AS max_pct,
               SUM(CASE WHEN a.qualifiedProfiles = 0 THEN 1 ELSE 0 END) AS empty_count
        FROM {AUDIENCE_TABLE} a
        JOIN {JOB_TABLE} j ON a.jobId = j.jobId AND a.sandboxName = j.sandboxName
        WHERE a.sandboxName = ?
          AND j.creationTime >= (SELECT MAX(creationTime) FROM {JOB_TABLE} WHERE sandboxName = ?) - INTERVAL '{days} days'
        GROUP BY a.audienceId
        ORDER BY avg_pct DESC NULLS LAST
    """
    params = [sandbox, sandbox]
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    if top_n:
        df = df.head(top_n)
    return df


def _enrich_with_segment_info(audience_rows_df, sandbox):
    """Attaches segment metadata to each audience row, grouped by the row's
    own job_date (point-in-time join - never 'latest available'). Honestly
    flags segment_info_found: False per-row when that day/sandbox has no
    aep_segment coverage (confirmed real gap: pre-2026-08-30 dates, and any
    non-b2b sandbox, currently have zero rows here)."""
    enriched = []
    for job_date, group in audience_rows_df.groupby("job_date"):
        seg_ids = group["audienceId"].unique().tolist()
        seg_info_df = _query_segment_info(sandbox, seg_ids, job_date)
        seg_info_by_id = {r["segmentId"]: r for _, r in seg_info_df.iterrows()} if len(seg_info_df) else {}
        for _, row in group.iterrows():
            seg_info = seg_info_by_id.get(row["audienceId"])
            entry = {
                "audience_id": row["audienceId"],
                "qualified_profiles": int(row["qualifiedProfiles"]),
                "pct_of_total_profiles": None if pd.isna(row["pctOfTotalProfiles"]) else float(row["pctOfTotalProfiles"]),
                "is_empty": int(row["qualifiedProfiles"]) == 0,
            }
            if seg_info is not None:
                entry.update({
                    "segment_info_found": True,
                    "segment_name": seg_info["segmentName"],
                    "lifecycle_state": seg_info["lifecycleState"],
                    "definition": seg_info["audienceDefinitionPqlText"],
                    "segment_type": seg_info["segmentType"],
                    "evaluation_type": seg_info["evaluationType"],
                    "tag_ids": list(seg_info["tag_ids"]),
                })
            else:
                entry.update({
                    "segment_info_found": False,
                    "segment_info_message": (
                        f"No segment definition available for {job_date} in '{sandbox}' - "
                        f"either this sandbox isn't loaded into the segment table yet, or "
                        f"this date is outside the segment table's history."
                    ),
                })
            enriched.append(entry)
    return enriched


def compute(sandbox: str, job_id: str = None, job_date: str = None,
            audience_id: str = None, segment_name: str = None,
            window_days: int = None, threshold_pct: float = DEFAULT_THRESHOLD_PCT,
            top_n: int = None):
    """
    job_id: exact job -> every audience in it, enriched with segment info
        and flagged above/below threshold_pct.
    job_date: a specific date -> resolves job(s) that ran that day (reuses
        segment_job_query's own date logic, including its honest
        "hasn't synced yet" fallback), then does the same per-job breakdown.
    audience_id / segment_name: pivot on ONE audience's identity across a
        window of jobs (window_days required). segment_name is matched
        case-insensitively and resolved to its ID(s) first; if it matches
        more than one distinct audience_id, ALL are returned separately -
        never silently collapsed to one.
    window_days + no audience_id/segment_name/job_id/job_date: ranks ALL
        audiences in that sandbox over the window (avg/max pct, how often
        empty) - for "which audience is consistently highest" questions.
    threshold_pct: default 30 (the agreed rule); a user can ask for a
        different cutoff (e.g. "above 50%") and it's honored as-is.
    top_n: limits the ranked/list outputs.
    """
    # --- Path 1: one specific job ---
    if job_id:
        df = _query_audiences_for_jobs(sandbox, [job_id])
        if len(df) == 0:
            return {"sandbox": sandbox, "job_id": job_id, "found": False,
                    "message": f"No job with ID '{job_id}' found in sandbox '{sandbox}', or it has no audiences recorded."}
        sql_used = df.attrs.get("sql_query_used", "")
        audiences = _enrich_with_segment_info(df, sandbox)
        for a in audiences:
            a["above_threshold"] = a["pct_of_total_profiles"] is not None and a["pct_of_total_profiles"] > threshold_pct
        above = [a for a in audiences if a["above_threshold"]]
        empty = [a for a in audiences if a["is_empty"]]
        return {
            "sandbox": sandbox, "job_id": job_id, "job_date": str(df["job_date"].iloc[0]),
            "job_status": df["job_status"].iloc[0], "total_profiles": int(df["totalProfiles"].iloc[0]),
            "found": True, "threshold_pct": threshold_pct,
            "audience_count": len(audiences), "above_threshold_count": len(above),
            "empty_count": len(empty),
            "audiences": audiences[:top_n] if top_n else audiences,
            "sql_query_used": sql_used,
        }

    # --- Path 2: a specific calendar date (possibly multiple jobs that day) ---
    if job_date is not None or (window_days is None and audience_id is None and segment_name is None and job_id is None):
        data_as_of = _query_latest_available_date(sandbox)
        if data_as_of is None:
            return {"sandbox": sandbox, "found": False,
                    "message": f"No job data found at all for sandbox '{sandbox}'."}

        fallback_used = False
        target_date = job_date or str(data_as_of)
        if job_date is not None and str(job_date) > str(data_as_of):
            fallback_used = True
            target_date = str(data_as_of)

        jobs_df = _query_jobs_by_date(sandbox, target_date)
        if len(jobs_df) == 0:
            return {"sandbox": sandbox, "requested_date": job_date, "data_as_of": str(data_as_of),
                    "target_date": target_date, "fallback_used": fallback_used,
                    "found": False, "jobs": [],
                    "message": (f"No job data has synced yet for {job_date} - most recent available "
                                f"is {data_as_of}." if fallback_used else f"No job ran in {sandbox} on {target_date}.")}

        job_ids = jobs_df["jobId"].tolist()
        aud_df = _query_audiences_for_jobs(sandbox, job_ids)
        sql_used = aud_df.attrs.get("sql_query_used", "") if len(aud_df) else ""
        audiences = _enrich_with_segment_info(aud_df, sandbox) if len(aud_df) else []
        for a in audiences:
            a["above_threshold"] = a["pct_of_total_profiles"] is not None and a["pct_of_total_profiles"] > threshold_pct

        jobs_out = []
        for jid in job_ids:
            job_audiences = [a for a in audiences if a["audience_id"] in aud_df[aud_df["jobId"] == jid]["audienceId"].tolist()]
            jobs_out.append({
                "job_id": jid,
                "audience_count": len(job_audiences),
                "above_threshold_count": sum(1 for a in job_audiences if a["above_threshold"]),
                "empty_count": sum(1 for a in job_audiences if a["is_empty"]),
                "audiences": job_audiences[:top_n] if top_n else job_audiences,
            })

        message = None
        if fallback_used:
            message = f"No job data has synced yet for {job_date} - showing {data_as_of} instead."

        return {
            "sandbox": sandbox, "requested_date": job_date, "data_as_of": str(data_as_of),
            "target_date": target_date, "fallback_used": fallback_used, "found": True,
            "threshold_pct": threshold_pct, "job_count": len(job_ids), "jobs": jobs_out,
            "message": message, "sql_query_used": sql_used,
        }

    # --- Path 3: one specific audience's history across a window ---
    if audience_id or segment_name:
        resolved_ids = [audience_id] if audience_id else _resolve_segment_name(sandbox, segment_name)
        if not resolved_ids:
            return {"sandbox": sandbox, "segment_name": segment_name, "audience_id": audience_id,
                    "found": False, "message": f"No segment named '{segment_name}' found in sandbox '{sandbox}'."}
        if window_days is None:
            return {"sandbox": sandbox, "found": False,
                    "message": "window_days is required to check an audience's history over time."}

        results = []
        for aid in resolved_ids:
            df = _query_audience_history(sandbox, aid, window_days)
            if len(df) == 0:
                results.append({"audience_id": aid, "found": False,
                                 "message": f"No job history found for audience '{aid}' in the last {window_days} days."})
                continue
            crossings = df[df["pctOfTotalProfiles"] > threshold_pct]
            results.append({
                "audience_id": aid, "found": True, "threshold_pct": threshold_pct,
                "window_days": window_days, "appearances": len(df),
                "ever_crossed_threshold": len(crossings) > 0, "times_crossed": len(crossings),
                "avg_pct_of_total_profiles": round(df["pctOfTotalProfiles"].mean(), 2),
                "max_pct_of_total_profiles": round(df["pctOfTotalProfiles"].max(), 2),
                "history": [
                    {"job_id": r["jobId"], "job_date": str(r["job_date"]),
                     "qualified_profiles": int(r["qualifiedProfiles"]),
                     "pct_of_total_profiles": None if pd.isna(r["pctOfTotalProfiles"]) else float(r["pctOfTotalProfiles"])}
                    for _, r in df.iterrows()
                ],
                "sql_query_used": df.attrs.get("sql_query_used", ""),
            })

        if len(results) == 1:
            out = results[0]
            out["sandbox"] = sandbox
            out["segment_name"] = segment_name
            return out
        return {"sandbox": sandbox, "segment_name": segment_name, "found": True,
                "multiple_matches": True, "match_count": len(results), "results": results}

    # --- Path 4: rank all audiences in a sandbox over a window ---
    df = _query_ranked_audiences(sandbox, window_days, top_n)
    sql_used = df.attrs.get("sql_query_used", "")
    if len(df) == 0:
        return {"sandbox": sandbox, "window_days": window_days, "found": False,
                "message": "No audience data found in that time period.", "sql_query_used": sql_used}

    ranked = [
        {"audience_id": r["audienceId"], "appearances": int(r["appearances"]),
         "avg_pct_of_total_profiles": round(r["avg_pct"], 2) if not pd.isna(r["avg_pct"]) else None,
         "max_pct_of_total_profiles": round(r["max_pct"], 2) if not pd.isna(r["max_pct"]) else None,
         "empty_count": int(r["empty_count"])}
        for _, r in df.iterrows()
    ]
    return {"sandbox": sandbox, "window_days": window_days, "threshold_pct": threshold_pct,
            "found": True, "audience_count": len(ranked), "audiences": ranked,
            "sql_query_used": sql_used}