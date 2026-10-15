import os
import json
from datetime import datetime
from dotenv import load_dotenv
from anthropic import AnthropicBedrock

load_dotenv()

client = AnthropicBedrock(
    api_key=os.getenv("BEDROCK_API_KEY"),
    aws_region=os.getenv("AWS_REGION"),
)
MODEL_ID = os.getenv("BEDROCK_MODEL_ID")

# The "menu" of available metrics. Adding a new metric later = one more entry here.
AVAILABLE_METRICS = [
    {
        "name": "guardrail_query",
        "description": (
            "Answers ANY question about guardrail/governance breach status - "
            "overall status, one specific guardrail, worst offenders, or "
            "breach history over a time period. Use this for all guardrail-"
            "related questions."
        ),
        "parameters": {
            "sandbox": "Which AEP sandbox. If not mentioned, return null.",
            "guardrail_name": "A single exact guardrail name (string), OR an array of 2+ names if the user wants to compare specific guardrails against each other (e.g. 'does X spike when Y spikes'). Return null for general questions.",
            "window_days": "Number of days to look back for history/trend questions. Use 1 for 'today', 2 for 'yesterday and today', 7 for 'this week' or 'last 7 days', 30 for 'this month'. Return null ONLY for pure 'right now, this exact instant' questions with no time word at all.",
            "sort_by": "'breach_count' if asking how OFTEN something breached, otherwise 'pct'.",
            "top_n": "Limit for ranked/worst-offender questions. Return null otherwise.",
            "compare_to_days_ago": "Number of days ago to compare against, ONLY if the user asks what's newly breaching / new compared to N days ago (default 30 if a range isn't given). Return null otherwise.",
        },
    },
       {
        "name": "segment_job_query",
        "description": (
            "Answers questions about AEP segmentation JOB executions - did a "
            "specific job succeed or fail, how long it took, which job(s) ran "
            "on a given day, reliability/performance over a time period "
            "(failure counts, trend by day, scheduler vs API comparison, "
            "by-sandbox comparison, which schedule runs most often, this "
            "period vs the previous period). Use this for job-level status/"
            "duration/reliability questions. Do NOT use this for listing or "
            "inspecting the AUDIENCES/segments inside a job, or an audience's "
            "qualified-profile percentage/threshold, or a segment's own "
            "definition/publish status - use segment_audience_query for those."
        ),
        "parameters": {
            "sandbox": "Which AEP sandbox. If not mentioned, return null. Ignored entirely when group_by is 'sandbox'.",
            "job_id": "An exact job ID/UUID, only if the user gives one directly. Return null otherwise.",
            "job_date": "A specific calendar date to check which job(s) ran, as 'YYYY-MM-DD'. Resolve relative phrases ('today', 'yesterday') using TODAY'S ACTUAL DATE given below. Return null for rolling-window/grouped/comparison questions.",
            "status": "Filter to one exact status: SUCCEEDED, FAILED, CANCELLED, CANCELLING, PROCESSING, QUEUED, or NEW. Return null for general questions.",
            "status_group": "'in_flight' for 'still running'/'in progress' questions, 'cancelled' for 'cancelled'/'stopped' questions. Return null otherwise. Do not use together with a specific 'status' value.",
            "window_days": "Number of days to look back. Use 7 for 'this week', 30 for 'this month'. Return null for a single-day or single-job lookup.",
            "source": "'scheduler' if asking specifically about scheduled/automatic runs, 'api' if asking specifically about manual/API-triggered runs. Return null otherwise.",
            "sort_by": "'duration' for slowest/longest-running jobs, 'profiles' for jobs handling the most profiles. Only meaningful with window_days.",
            "top_n": "Limit for ranked questions. Return null otherwise.",
            "group_by": "'day' for a trend/over-time breakdown, 'source' to compare scheduler vs API, 'schedule' to compare by scheduleId (e.g. 'which schedule runs most often'), 'sandbox' to compare across ALL sandboxes (e.g. 'success rate by sandbox'). Return null for a single-sandbox, non-grouped question.",
            "compare_previous_period": "true ONLY if the user explicitly compares this period to the immediately preceding one of the same length (e.g. 'this month vs last month', 'this week vs last week'). Otherwise false.",
        },
    },
        {
        "name": "segment_audience_query",
        "description": (
            "Answers questions about the AUDIENCES/segments inside a "
            "segmentation job - listing them, their qualifiedProfiles count, "
            "whether they're above/below a profile-percentage threshold "
            "(default 30%), empty audiences, or a specific named audience/"
            "segment's own definition, publish status, and history across "
            "jobs over time (e.g. 'has this audience ever crossed 50%'). "
            "Use this for any audience/segment-level question, as opposed to "
            "job-level status/duration questions (use segment_job_query for "
            "those instead)."
        ),
        "parameters": {
            "sandbox": "Which AEP sandbox. If not mentioned, return null.",
            "job_id": "An exact job ID/UUID to list/inspect its audiences. Return null otherwise.",
            "job_date": "A specific calendar date ('YYYY-MM-DD') to inspect audiences across whichever job(s) ran that day. Resolve relative phrases using TODAY'S ACTUAL DATE below. Return null for single-job or single-audience-history questions.",
            "audience_id": "An exact audience/segment ID (GUID), only if the user gives one directly. Return null otherwise.",
            "segment_name": "A segment/audience's human-readable name, if the user refers to it by name rather than ID. Return null otherwise.",
            "window_days": "Required when audience_id or segment_name is set, to check its history over time (e.g. 'in the last 30 days'). Also used for general 'which audience is consistently highest' ranking questions with no specific job/audience given.",
            "threshold_pct": "The percentage cutoff for 'above/below threshold' questions. Default to 30 unless the user explicitly states a different number (e.g. 'above 50%').",
            "top_n": "Limit for ranked/list questions. Return null otherwise.",
        },
    },
]


def _build_router_system_prompt() -> str:
    # Built fresh per call (not a module-level constant) so "today" is always
    # the real current date - this process can stay running for days
    # (Flask/MCP server), and a baked-in date would silently mis-resolve
    # "today"/"yesterday" job questions after the first day.
    today_str = datetime.now().strftime("%Y-%m-%d")
    return f"""You are a routing engine for a platform operations assistant.

Today's actual date is {today_str}. Use this as "today" when resolving any
relative date phrase ("today", "yesterday", "this week") - never guess or
assume a different date.

You will be given a user's question. Your ONLY job is to:
1. Decide which metric below (if any) answers the question.
2. Extract parameters based only on what the user actually said.
3. If the question asks whether one guardrail's behavior relates to another (e.g. "when X spikes does Y spike too"), set guardrail_name to an array containing both exact names.
4. If the question asks what's newly breaching / wasn't a problem N days ago, set compare_to_days_ago (default 30 if unspecified) and leave guardrail_name/window_days null.
5. If asking about a specific segmentation job by its exact ID, set job_id and leave the other segment_job_query parameters null.
6. If asking about a specific day ("today", "yesterday", an explicit date) for job status, resolve to job_date in YYYY-MM-DD format using today's actual date above - do not guess a different date.
7. If asking about job reliability/failures/performance over a period (not one specific day), set window_days instead of job_date.
8. If asking about the audiences/segments INSIDE a job (listing them, their qualified profile %, threshold, empty ones), or a specific audience/segment's own definition/publish-status/history, use segment_audience_query instead of segment_job_query.

Available metrics (the ONLY valid options):
{json.dumps(AVAILABLE_METRICS, indent=2)}

Rules:
- If the question doesn't match any metric above, set "matched_metric" to null.
- Never guess a parameter value the user didn't state or clearly imply. Use null for anything not mentioned.
- If the question implies "worst"/"closest to breaching"/"top" guardrails, set top_n to 5 by default unless a specific number is given.
- If a time range is mentioned but no exact number of days, default window_days to 7.
- Respond with ONLY valid JSON, no other text, no markdown formatting, no code fences.

Respond in exactly this JSON shape:
{{
  "matched_metric": "metric_name_or_null",
  "parameters": {{"sandbox": null}},
  "reasoning": "one short sentence explaining the match or non-match"
}}
"""


def route(user_question: str) -> dict:
    response = client.messages.create(
        model=MODEL_ID,
        max_tokens=1024,
        system=_build_router_system_prompt(),
        messages=[{"role": "user", "content": user_question}],
    )

    text_blocks = [block.text for block in response.content if block.type == "text"]
    raw_text = "".join(text_blocks).strip()

    if raw_text.startswith("```"):
        raw_text = raw_text.strip("`")
        if raw_text.startswith("json"):
            raw_text = raw_text[4:]
        raw_text = raw_text.strip()

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        return {
            "matched_metric": None,
            "parameters": {},
            "reasoning": f"Router returned unparseable output (stop_reason={response.stop_reason}): {raw_text!r}",
        }


if __name__ == "__main__":
    test_question = "Are there open guardrail or governance breaches?"
    result = route(test_question)
    print(json.dumps(result, indent=2))