import duckdb
import pandas as pd
from datetime import timedelta

DB_PATH = "db/prism.duckdb"
TABLE_NAME = "aep_segment_job_last_45_days"

# Full status enum per the schema doc. Only SUCCEEDED/FAILED/CANCELLED/PROCESSING
# have been observed in the current 45-day snapshot, but NEW/QUEUED/CANCELLING
# are real possible values - handle them generically, don't hardcode only the
# four seen so far.
IN_FLIGHT_STATUSES = ["NEW", "QUEUED", "PROCESSING"]
CANCELLED_STATUSES = ["CANCELLING", "CANCELLED"]

GROUP_BY_OPTIONS = {"day", "source", "sandbox", "schedule"}
STATUS_GROUP_OPTIONS = {"in_flight", "cancelled"}


def _status_category(status):
    if status is None:
        return "unknown"
    s = str(status).upper()
    if s == "SUCCEEDED":
        return "succeeded"
    if s == "FAILED":
        return "failed"
    if s in CANCELLED_STATUSES:
        return "cancelled"
    if s in IN_FLIGHT_STATUSES:
        return "in_flight"
    return "unknown"


def _clean_ts(val):
    """jobEndTime (and job_audiences' start/endTime) arrive from source as the
    literal 4-character string 'null' when genuinely absent - NOT a real SQL
    NULL. Confirmed directly against the loaded data. Treat both the same so
    'is this job finished' checks don't silently misread a string as real."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() == "null":
        return None
    return s


def _ms_to_human(ms):
    if ms is None or pd.isna(ms):
        return None
    seconds = float(ms) / 1000.0
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60.0
    if minutes < 60:
        return f"{minutes:.1f} min"
    return f"{minutes / 60.0:.1f} hours"


def _resolve_sql_for_display(query: str, params: list) -> str:
    resolved = query
    for p in params:
        display_val = f"'{p}'" if isinstance(p, str) else str(p)
        resolved = resolved.replace("?", display_val, 1)
    return resolved.strip()


def _build_filter_clause(status=None, source=None, status_group=None):
    """status/source are matched case-insensitively since the router/user may
    not match exact casing. status_group expands to the full IN_FLIGHT/
    CANCELLED status list - lets a user ask about 'jobs still running' or
    'cancelled jobs' without knowing the exact enum value."""
    clause = ""
    params = []
    if status:
        clause += " AND UPPER(status) = UPPER(?)"
        params.append(status)
    if source:
        clause += " AND LOWER(source) = LOWER(?)"
        params.append(source)
    if status_group == "in_flight":
        placeholders = ", ".join(["?"] * len(IN_FLIGHT_STATUSES))
        clause += f" AND UPPER(status) IN ({placeholders})"
        params.extend(IN_FLIGHT_STATUSES)
    elif status_group == "cancelled":
        placeholders = ", ".join(["?"] * len(CANCELLED_STATUSES))
        clause += f" AND UPPER(status) IN ({placeholders})"
        params.extend(CANCELLED_STATUSES)
    return clause, params


# NOTE on timestamps: TIMESTAMP WITH TIME ZONE columns are cast to VARCHAR in
# every query below, since this venv is missing `pytz` (needed by duckdb's
# fetchdf() to convert TIMESTAMPTZ -> pandas). Harmless to leave in place.

def _query_job_by_id(sandbox, job_id):
    query = f"""
        SELECT jobId, sandboxName, status, source, batchId,
            totalTimeMs, segmentationTimeMs,
            CAST(jobStartTime AS VARCHAR) AS jobStartTime, jobEndTime,
            totalProfiles, audienceCount, scheduleId, runId, mergePolicyId,
            CAST(creationTime AS VARCHAR) AS creationTime,
            CAST(updateTime AS VARCHAR) AS updateTime
        FROM {TABLE_NAME}
        WHERE sandboxName = ? AND jobId = ?
    """
    params = [sandbox, job_id]
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_latest_available_date(sandbox):
    """MAX calendar date actually present for this sandbox."""
    query = f"SELECT MAX(CAST(creationTime AS DATE)) FROM {TABLE_NAME} WHERE sandboxName = ?"
    con = duckdb.connect(DB_PATH)
    result = con.execute(query, [sandbox]).fetchone()[0]
    con.close()
    return result


def _query_earliest_available_date(sandbox):
    """MIN calendar date actually present - used to detect when a requested
    comparison window reaches further back than the data actually goes
    (confirmed real: only ~46 days of history exist total)."""
    query = f"SELECT MIN(CAST(creationTime AS DATE)) FROM {TABLE_NAME} WHERE sandboxName = ?"
    con = duckdb.connect(DB_PATH)
    result = con.execute(query, [sandbox]).fetchone()[0]
    con.close()
    return result


def _query_jobs_by_date(sandbox, target_date, status=None, source=None, status_group=None):
    filter_clause, filter_params = _build_filter_clause(status, source, status_group)
    query = f"""
        SELECT jobId, sandboxName, status, source, totalTimeMs, segmentationTimeMs,
            CAST(jobStartTime AS VARCHAR) AS jobStartTime, jobEndTime,
            totalProfiles, audienceCount, scheduleId,
            CAST(creationTime AS VARCHAR) AS creationTime
        FROM {TABLE_NAME}
        WHERE sandboxName = ?
          AND CAST(creationTime AS DATE) = ?
          {filter_clause}
        ORDER BY creationTime DESC
    """
    params = [sandbox, str(target_date)] + filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_windowed(sandbox, days, status=None, source=None, status_group=None):
    """FIXED: previously omitted jobStartTime/jobEndTime/scheduleId, which
    crashed _job_detail_dict() with a KeyError on any 'last N days' question.
    Now selects everything _job_detail_dict() needs."""
    days = int(days)
    filter_clause, filter_params = _build_filter_clause(status, source, status_group)
    query = f"""
        SELECT jobId, sandboxName, status, source, totalTimeMs, segmentationTimeMs,
            CAST(jobStartTime AS VARCHAR) AS jobStartTime, jobEndTime,
            totalProfiles, audienceCount, scheduleId,
            CAST(creationTime AS VARCHAR) AS creationTime
        FROM {TABLE_NAME}
        WHERE sandboxName = ?
          AND creationTime >= (SELECT MAX(creationTime) FROM {TABLE_NAME} WHERE sandboxName = ?) - INTERVAL '{days} days'
          {filter_clause}
        ORDER BY creationTime DESC
    """
    params = [sandbox, sandbox] + filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_windowed_previous_period(sandbox, days, status=None, source=None, status_group=None):
    """The N-day period immediately BEFORE the current N-day window, with no
    gap - e.g. days=30 gives 'last month' when _query_windowed gives 'this
    month'. Used only by compare_previous_period."""
    days = int(days)
    filter_clause, filter_params = _build_filter_clause(status, source, status_group)
    query = f"""
        SELECT jobId, sandboxName, status, source, totalTimeMs, segmentationTimeMs,
            CAST(jobStartTime AS VARCHAR) AS jobStartTime, jobEndTime,
            totalProfiles, audienceCount, scheduleId,
            CAST(creationTime AS VARCHAR) AS creationTime
        FROM {TABLE_NAME}
        WHERE sandboxName = ?
          AND creationTime >= (SELECT MAX(creationTime) FROM {TABLE_NAME} WHERE sandboxName = ?) - INTERVAL '{2 * days} days'
          AND creationTime <  (SELECT MAX(creationTime) FROM {TABLE_NAME} WHERE sandboxName = ?) - INTERVAL '{days} days'
          {filter_clause}
        ORDER BY creationTime DESC
    """
    params = [sandbox, sandbox, sandbox] + filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_grouped_by_day(sandbox, days, status=None, source=None, status_group=None):
    days = int(days)
    filter_clause, filter_params = _build_filter_clause(status, source, status_group)
    query = f"""
        SELECT CAST(creationTime AS DATE) AS group_key,
               COUNT(*) AS job_count,
               SUM(CASE WHEN UPPER(status) = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_count,
               SUM(CASE WHEN UPPER(status) = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
               AVG(totalTimeMs) AS avg_total_time_ms,
               SUM(totalProfiles) AS total_profiles_sum,
               AVG(audienceCount) AS avg_audience_count
        FROM {TABLE_NAME}
        WHERE sandboxName = ?
          AND creationTime >= (SELECT MAX(creationTime) FROM {TABLE_NAME} WHERE sandboxName = ?) - INTERVAL '{days} days'
          {filter_clause}
        GROUP BY 1
        ORDER BY 1 ASC
    """
    params = [sandbox, sandbox] + filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_grouped_by_source(sandbox, days, status=None, status_group=None):
    days = int(days)
    filter_clause, filter_params = _build_filter_clause(status, None, status_group)
    query = f"""
        SELECT source AS group_key,
               COUNT(*) AS job_count,
               SUM(CASE WHEN UPPER(status) = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_count,
               SUM(CASE WHEN UPPER(status) = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
               AVG(totalTimeMs) AS avg_total_time_ms,
               SUM(totalProfiles) AS total_profiles_sum,
               AVG(audienceCount) AS avg_audience_count
        FROM {TABLE_NAME}
        WHERE sandboxName = ?
          AND creationTime >= (SELECT MAX(creationTime) FROM {TABLE_NAME} WHERE sandboxName = ?) - INTERVAL '{days} days'
          {filter_clause}
        GROUP BY 1
        ORDER BY 1 ASC
    """
    params = [sandbox, sandbox] + filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_grouped_by_schedule(sandbox, days, status=None, source=None, status_group=None):
    days = int(days)
    filter_clause, filter_params = _build_filter_clause(status, source, status_group)
    query = f"""
        SELECT scheduleId AS group_key,
               COUNT(*) AS job_count,
               SUM(CASE WHEN UPPER(status) = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_count,
               SUM(CASE WHEN UPPER(status) = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
               AVG(totalTimeMs) AS avg_total_time_ms,
               SUM(totalProfiles) AS total_profiles_sum,
               AVG(audienceCount) AS avg_audience_count
        FROM {TABLE_NAME}
        WHERE sandboxName = ?
          AND creationTime >= (SELECT MAX(creationTime) FROM {TABLE_NAME} WHERE sandboxName = ?) - INTERVAL '{days} days'
          {filter_clause}
        GROUP BY 1
        ORDER BY job_count DESC
    """
    params = [sandbox, sandbox] + filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _query_grouped_by_sandbox(days, status=None, source=None, status_group=None):
    """Crosses ALL sandboxes - no single-sandbox filter, anchored on the
    global latest creationTime across the whole table."""
    days = int(days)
    filter_clause, filter_params = _build_filter_clause(status, source, status_group)
    query = f"""
        SELECT sandboxName AS group_key,
               COUNT(*) AS job_count,
               SUM(CASE WHEN UPPER(status) = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_count,
               SUM(CASE WHEN UPPER(status) = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
               AVG(totalTimeMs) AS avg_total_time_ms,
               SUM(totalProfiles) AS total_profiles_sum,
               AVG(audienceCount) AS avg_audience_count
        FROM {TABLE_NAME}
        WHERE creationTime >= (SELECT MAX(creationTime) FROM {TABLE_NAME}) - INTERVAL '{days} days'
          {filter_clause}
        GROUP BY 1
        ORDER BY 1 ASC
    """
    params = filter_params
    con = duckdb.connect(DB_PATH)
    df = con.execute(query, params).fetchdf()
    con.close()
    df.attrs["sql_query_used"] = _resolve_sql_for_display(query, params)
    return df


def _bucket_rows(df):
    buckets = []
    for _, r in df.iterrows():
        total = int(r["job_count"])
        failed = int(r["failed_count"])
        buckets.append({
            "group_value": str(r["group_key"]),
            "job_count": total,
            "succeeded_count": int(r["succeeded_count"]),
            "failed_count": failed,
            "failure_rate_pct": round((failed / total) * 100, 1) if total else 0,
            "avg_total_time_ms": None if pd.isna(r["avg_total_time_ms"]) else round(r["avg_total_time_ms"], 1),
            "total_profiles_sum": None if pd.isna(r["total_profiles_sum"]) else int(r["total_profiles_sum"]),
            "avg_audience_count": None if pd.isna(r["avg_audience_count"]) else round(r["avg_audience_count"], 1),
        })
    return buckets


def _job_detail_dict(row):
    return {
        "job_id": row["jobId"],
        "status": row["status"],
        "status_category": _status_category(row["status"]),
        "source": row["source"],
        "schedule_id": row["scheduleId"],
        "creation_time": row["creationTime"],
        "job_start_time": row["jobStartTime"],
        "job_end_time": _clean_ts(row["jobEndTime"]),
        "total_time_ms": None if pd.isna(row["totalTimeMs"]) else int(row["totalTimeMs"]),
        "total_time_human": _ms_to_human(row["totalTimeMs"]),
        "total_profiles": None if pd.isna(row["totalProfiles"]) else int(row["totalProfiles"]),
        "audience_count": None if pd.isna(row["audienceCount"]) else int(row["audienceCount"]),
    }


def _summarize_jobs_df(df, sort_by="duration", top_n=None):
    """Shared by the plain windowed view and compare_previous_period, so both
    compute stats identically instead of two call sites quietly drifting
    apart (the same copy-paste-drift problem flagged earlier elsewhere)."""
    total = len(df)
    status_counts = df["status"].value_counts().to_dict()
    failed = status_counts.get("FAILED", 0)
    succeeded = status_counts.get("SUCCEEDED", 0)
    cancelled = sum(status_counts.get(s, 0) for s in CANCELLED_STATUSES)
    in_flight = sum(status_counts.get(s, 0) for s in IN_FLIGHT_STATUSES)

    sort_col = "totalProfiles" if sort_by == "profiles" else "totalTimeMs"
    ranked = df.sort_values(sort_col, ascending=False, na_position="last")
    jobs = [_job_detail_dict(r) for _, r in ranked.iterrows()]

    return {
        "job_count": total, "succeeded_count": succeeded, "failed_count": failed,
        "cancelled_count": cancelled, "in_flight_count": in_flight,
        "failure_rate_pct": round((failed / total) * 100, 1) if total else 0,
        "avg_total_time_ms": None if df["totalTimeMs"].isna().all() else round(df["totalTimeMs"].mean(), 1),
        "max_total_time_ms": None if df["totalTimeMs"].isna().all() else int(df["totalTimeMs"].max()),
        "total_profiles_sum": None if df["totalProfiles"].isna().all() else int(df["totalProfiles"].sum()),
        "avg_audience_count": None if df["audienceCount"].isna().all() else round(df["audienceCount"].mean(), 1),
        "jobs": jobs[:top_n] if top_n else jobs,
    }


def compute(sandbox: str, job_id: str = None, job_date: str = None, status: str = None,
            status_group: str = None, window_days: int = None, source: str = None,
            sort_by: str = "duration", top_n: int = None, group_by: str = None,
            compare_previous_period: bool = False):
    """
    job_id: exact job ID lookup - overrides everything else when set.
    job_date: a specific calendar date ('YYYY-MM-DD'). None + nothing else
        set = latest available date for this sandbox.
    status: filter to one exact status. Case-insensitive.
    status_group: 'in_flight' (NEW/QUEUED/PROCESSING) or 'cancelled'
        (CANCELLING/CANCELLED) - for "still running" / "cancelled" questions
        without needing the exact enum value.
    window_days: aggregate/reliability view over N days.
    source: 'scheduler' or 'api'. Case-insensitive.
    sort_by: 'duration' (totalTimeMs desc) or 'profiles' (totalProfiles desc).
    top_n: limit the ranked list.
    group_by: 'day' (trend), 'source' (scheduler vs api), 'schedule' (by
        scheduleId, ordered by frequency), or 'sandbox' (compares ALL
        sandboxes - the sandbox parameter is ignored in this mode, flagged
        via a 'note' field in the result). Requires window_days (defaults
        to 7 if omitted).
    compare_previous_period: if True, compares the current window_days-long
        period against the equal-length period immediately before it (e.g.
        "this month vs last month"). Flags previous_period_incomplete if the
        data doesn't go back far enough to cover the earlier period fully.
    """
    if job_id:
        df = _query_job_by_id(sandbox, job_id)
        sql_used = df.attrs.get("sql_query_used", "")
        if len(df) == 0:
            return {"sandbox": sandbox, "job_id": job_id, "found": False,
                    "message": f"No job with ID '{job_id}' found in sandbox '{sandbox}'.",
                    "sql_query_used": sql_used}
        row = df.iloc[0]
        result = _job_detail_dict(row)
        result.update({"sandbox": sandbox, "found": True, "sql_query_used": sql_used})
        return result

    if group_by is not None and group_by not in GROUP_BY_OPTIONS:
        return {"sandbox": sandbox, "found": False,
                "message": f"Unknown group_by '{group_by}'. Valid options: day, source, sandbox, schedule."}
    if status_group is not None and status_group not in STATUS_GROUP_OPTIONS:
        return {"sandbox": sandbox, "found": False,
                "message": f"Unknown status_group '{status_group}'. Valid options: in_flight, cancelled."}

    if group_by is not None:
        if window_days is None:
            window_days = 7

        if group_by == "day":
            df = _query_grouped_by_day(sandbox, window_days, status, source, status_group)
        elif group_by == "source":
            df = _query_grouped_by_source(sandbox, window_days, status, status_group)
        elif group_by == "schedule":
            df = _query_grouped_by_schedule(sandbox, window_days, status, source, status_group)
        else:  # "sandbox"
            df = _query_grouped_by_sandbox(window_days, status, source, status_group)

        sql_used = df.attrs.get("sql_query_used", "")
        if len(df) == 0:
            return {"sandbox": sandbox, "window_days": window_days, "grouped_by": group_by,
                    "found": False, "buckets": [],
                    "message": "No jobs matched those filters in that time period.",
                    "sql_query_used": sql_used}

        buckets = _bucket_rows(df)
        result = {
            "window_days": window_days, "grouped_by": group_by, "status": status,
            "source": source, "status_group": status_group, "found": True,
            "bucket_count": len(buckets), "buckets": buckets[:top_n] if top_n else buckets,
            "sql_query_used": sql_used,
        }
        if group_by == "sandbox":
            result["sandbox"] = None
            result["note"] = "Comparing across all sandboxes - the single-sandbox selection is ignored for this view."
        else:
            result["sandbox"] = sandbox
        return result

    if compare_previous_period:
        if window_days is None:
            window_days = 7

        current_df = _query_windowed(sandbox, window_days, status, source, status_group)
        previous_df = _query_windowed_previous_period(sandbox, window_days, status, source, status_group)
        data_as_of = _query_latest_available_date(sandbox)
        earliest = _query_earliest_available_date(sandbox)

        current_summary = _summarize_jobs_df(current_df, sort_by, top_n) if len(current_df) else None
        previous_summary = _summarize_jobs_df(previous_df, sort_by, top_n) if len(previous_df) else None

        previous_incomplete = False
        previous_period_message = None
        if data_as_of is not None:
            needed_start = data_as_of - timedelta(days=2 * window_days)
            if earliest is None or earliest > needed_start:
                previous_incomplete = True
                total_days = (data_as_of - earliest).days + 1 if earliest else 0
                previous_period_message = (
                    f"Only {total_days} days of job history exist in total - the previous "
                    f"{window_days}-day period falls partially or fully outside that range, "
                    f"so this comparison may be incomplete."
                )

        change = None
        if current_summary and previous_summary:
            change = {
                "job_count_change": current_summary["job_count"] - previous_summary["job_count"],
                "failure_rate_pct_change": round(current_summary["failure_rate_pct"] - previous_summary["failure_rate_pct"], 1),
                "avg_total_time_ms_change": (
                    None if current_summary["avg_total_time_ms"] is None or previous_summary["avg_total_time_ms"] is None
                    else round(current_summary["avg_total_time_ms"] - previous_summary["avg_total_time_ms"], 1)
                ),
            }

        return {
            "sandbox": sandbox, "window_days": window_days, "compare_previous_period": True,
            "data_as_of": str(data_as_of) if data_as_of else None,
            "previous_period_incomplete": previous_incomplete,
            "previous_period_message": previous_period_message,
            "current_period": current_summary or {"job_count": 0},
            "previous_period": previous_summary or {"job_count": 0},
            "change": change,
            "sql_query_used": current_df.attrs.get("sql_query_used", ""),
        }

    # status/status_group filter alone (no date/window given) implies a
    # time-bounded question - default to 7 days, same precedent as
    # guardrail_query's window_days default.
    if window_days is None and job_date is None and (status is not None or status_group is not None):
        window_days = 7

    if window_days is not None:
        df = _query_windowed(sandbox, window_days, status, source, status_group)
        sql_used = df.attrs.get("sql_query_used", "")
        data_as_of = _query_latest_available_date(sandbox)

        if len(df) == 0:
            return {"sandbox": sandbox, "window_days": window_days, "status": status,
                    "source": source, "status_group": status_group, "found": False,
                    "job_count": 0, "jobs": [],
                    "data_as_of": str(data_as_of) if data_as_of else None,
                    "message": "No jobs matched those filters in that time period.",
                    "sql_query_used": sql_used}

        summary = _summarize_jobs_df(df, sort_by, top_n)
        return {
            "sandbox": sandbox, "window_days": window_days, "status": status, "source": source,
            "status_group": status_group, "found": True,
            "data_as_of": str(data_as_of) if data_as_of else None,
            **summary,
            "sql_query_used": sql_used,
        }

    # job_date given, or nothing given at all (defaults to latest available date)
    data_as_of = _query_latest_available_date(sandbox)
    if data_as_of is None:
        return {"sandbox": sandbox, "found": False,
                "message": f"No job data found at all for sandbox '{sandbox}'."}

    fallback_used = False
    target_date = job_date or str(data_as_of)
    if job_date is not None and str(job_date) > str(data_as_of):
        fallback_used = True
        target_date = str(data_as_of)

    df = _query_jobs_by_date(sandbox, target_date, status, source, status_group)
    sql_used = df.attrs.get("sql_query_used", "")
    jobs = [_job_detail_dict(r) for _, r in df.iterrows()]

    message = None
    if fallback_used:
        message = (f"No job data has synced yet for {job_date} - showing the "
                   f"most recently available day ({data_as_of}) instead.")
    elif len(jobs) == 0:
        message = f"No job ran in {sandbox} on {target_date}."

    total_profiles_sum = sum(j["total_profiles"] for j in jobs if j["total_profiles"] is not None) if jobs else None
    total_audience_count_sum = sum(j["audience_count"] for j in jobs if j["audience_count"] is not None) if jobs else None

    return {
        "sandbox": sandbox, "requested_date": job_date, "data_as_of": str(data_as_of),
        "target_date": target_date, "fallback_used": fallback_used,
        "found": len(jobs) > 0, "job_count": len(jobs), "jobs": jobs,
        "total_profiles_sum": total_profiles_sum, "total_audience_count_sum": total_audience_count_sum,
        "message": message, "sql_query_used": sql_used,
    }