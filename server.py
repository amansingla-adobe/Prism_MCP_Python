from fastmcp import FastMCP
from metrics.guardrail_query import compute as compute_guardrail_query
from metrics.segment_job_query import compute as compute_segment_job_query
from metrics.segment_audience_query import compute as compute_segment_audience_query

mcp = FastMCP("Project Prism - Platform Ops")


@mcp.tool
def query_guardrails(
    sandbox: str,
    guardrail_name=None,
    window_days: int = None,
    sort_by: str = "pct",
    top_n: int = None,
    compare_to_days_ago: int = None,
) -> dict:
    """
    Query guardrail/governance breach status for an AEP sandbox. Flexible -
    combine parameters as needed:

    - Overall status right now: leave guardrail_name and window_days empty.
    - One specific guardrail's status: set guardrail_name to a single name
      (e.g. "Streaming Ingestion").
    - Compare two or more specific guardrails against each other (e.g. "does
      X spike when Y spikes"): set guardrail_name to a LIST of exact names
      (e.g. ["Streaming Ingestion", "Streaming Segmentation"]).
    - Worst offenders / ranking: set top_n (and sort_by="breach_count" if
      ranking by how OFTEN something breached, rather than by peak %).
    - Breach history over time: set window_days (e.g. 7 for "last 7 days").
      Can be combined with a single guardrail_name or a comparison list.
    - What's newly breaching that wasn't a problem N days ago: set
      compare_to_days_ago (e.g. 30). This overrides guardrail_name and
      window_days when set.

    Note: Edge Segmentation and Live Inbound Actions are automatically
    excluded from general (all-guardrail) results, since their production
    metrics aren't available yet. Asking about them by name directly will
    return a clear "not yet available" message instead of a misleading 0%.
    """
    if compare_to_days_ago:
        return compute_guardrail_query(sandbox=sandbox, compare_to_days_ago=compare_to_days_ago)

    kwargs = {"sandbox": sandbox, "sort_by": sort_by}
    if guardrail_name:
        kwargs["guardrail_name"] = guardrail_name
    if window_days:
        kwargs["window_days"] = window_days
    if top_n:
        kwargs["top_n"] = top_n

    return compute_guardrail_query(**kwargs)


@mcp.tool
def query_segment_jobs(
    sandbox: str,
    job_id: str = None,
    job_date: str = None,
    status: str = None,
    window_days: int = None,
    source: str = None,
    sort_by: str = "duration",
    top_n: int = None,
) -> dict:
    """
    Query AEP segmentation job execution status, duration, and reliability.

    - A specific job's status/duration: set job_id to its exact ID.
    - Which job(s) ran on a specific day: set job_date ('YYYY-MM-DD'). If
      that date hasn't synced into the data yet, the result honestly
      reports the most recent available date instead (fallback_used: true)
      rather than claiming no jobs ran.
    - Reliability/performance over a period (failure rate, slowest jobs):
      set window_days (e.g. 7 for "last 7 days").
    - Filter to one outcome: set status (SUCCEEDED/FAILED/CANCELLED/
      CANCELLING/PROCESSING/QUEUED/NEW).
    - Filter to scheduled vs manual runs: set source ('scheduler' or 'api').
    - Ranking: combine window_days with top_n and sort_by ('duration' or
      'profiles').
    """
    kwargs = {"sandbox": sandbox, "sort_by": sort_by}
    if job_id:
        kwargs["job_id"] = job_id
    if job_date:
        kwargs["job_date"] = job_date
    if status:
        kwargs["status"] = status
    if window_days:
        kwargs["window_days"] = window_days
    if source:
        kwargs["source"] = source
    if top_n:
        kwargs["top_n"] = top_n

    return compute_segment_job_query(**kwargs)


@mcp.tool
def query_segment_audiences(
    sandbox: str,
    job_id: str = None,
    job_date: str = None,
    audience_id: str = None,
    segment_name: str = None,
    window_days: int = None,
    threshold_pct: float = 30.0,
    top_n: int = None,
) -> dict:
    """
    Query the AUDIENCES/segments inside a segmentation job, or a specific
    audience/segment's own definition and history over time.

    - List every audience in one job, with qualifiedProfiles and
      above/below-threshold flags: set job_id.
    - List audiences across whichever job(s) ran on a specific day: set
      job_date ('YYYY-MM-DD'). Honestly reports if that date hasn't synced
      yet (same fallback behavior as query_segment_jobs).
    - A named/specific audience's own definition, publish status, and
      history across jobs: set audience_id (exact GUID) or segment_name
      (human-readable name), plus window_days (e.g. 7, 30) to check things
      like "has this ever crossed 50% in the last 30 days".
    - Rank all audiences in a sandbox over a window (no job/audience given):
      set window_days alone - for "which audience is consistently highest".

    threshold_pct defaults to 30 (the team's standard cutoff for "above
    threshold") but can be overridden if the user asks for a different cutoff.

    Note: segment definition/publish-status/tag info is only available for
    sandboxes and dates the aep_segment table actually covers (currently
    b2b-* sandboxes, last ~30 days). Outside that range, qualifiedProfiles/
    pct_of_total_profiles still return correctly, but segment_info_found
    will be false with an honest explanation instead of a guess.
    """
    kwargs = {"sandbox": sandbox, "threshold_pct": threshold_pct}
    if job_id:
        kwargs["job_id"] = job_id
    if job_date:
        kwargs["job_date"] = job_date
    if audience_id:
        kwargs["audience_id"] = audience_id
    if segment_name:
        kwargs["segment_name"] = segment_name
    if window_days:
        kwargs["window_days"] = window_days
    if top_n:
        kwargs["top_n"] = top_n

    return compute_segment_audience_query(**kwargs)

if __name__ == "__main__":
    mcp.run()