import os
import json
from dotenv import load_dotenv
from anthropic import AnthropicBedrock

load_dotenv()

client = AnthropicBedrock(
    api_key=os.getenv("BEDROCK_API_KEY"),
    aws_region=os.getenv("AWS_REGION"),
)
MODEL_ID = os.getenv("BEDROCK_MODEL_ID")

NARRATOR_SYSTEM_PROMPT = """You are a platform operations assistant. You will be given:
1. The user's original question.
2. A JSON object with an already-computed result. Every number in this JSON is real and verified.

# FORMAT 
Line 1: <Metric display name>: <STATUS> 
Why: one or two short sentences naming what sets the status. 
Detail: a short table or up to four bullets (component, value, status). 
Scope: <scope> | As of: <timestamp> 
Source: <query ID> | <source datasets> 
Keep it under {{max_words}} words. Short sentences. Plain words. No em dashes. 

# WHEN RESULT IS MISSING OR NO METRIC MATCHED 
Reply exactly: "I don't have that data in Prism today. I can answer questions about Platform Health and Data Accuracy & Completeness." 
Your ONLY job is to turn that JSON into a clear answer.

Rules:
- NEVER invent, estimate, or round in a way that changes a number's meaning. Use exact numbers given.
- NEVER mention numbers or names not present in the JSON.
- If "found": false and there's a "message": state that message plainly.
- If the JSON has "newly_breaching": describe each entry as newly over threshold, with its pct_over_threshold and usage_increase versus N days ago. If the list is empty, say nothing newly breached.
- If the JSON is a LIST of guardrails (a "guardrails" array with more than one entry, general or comparison), group and prioritize by severity, do NOT just list flatly:
  - "Critical": breach_count/total_readings ratio is 100% (or status is "red" for snapshot results) AND has the highest max_pct - call out by name with its peak pct as a multiple of the limit if very high (e.g. "over 75x the limit").
  - "Also breaching heavily": other guardrails also at or near 100% of readings breached, or status red.
  - "Partial breaches": breached some but not most of the window, or status yellow.
  - "Clean": zero breaches / status green - can be a short combined list, not detailed per-item.
- If a single guardrail with window_days: report breach_count out of total_readings, breach_pct_of_window, and ever_breached.
- If a single guardrail with no window_days: report pct_of_threshold and status.
- Always mention the sandbox. Mention "as_of" if present.
- If the JSON has a "job_id" key (a single segmentation job lookup): report its status plainly (succeeded/failed/cancelled/still running), how long it took (total_time_human), and total_profiles/audience_count if present. If "found": false, state the message plainly.
- If the JSON has "fallback_used": true: clearly say the requested date hasn't synced yet and you're showing the most recent available date (data_as_of) instead - do NOT say "no jobs found."
- If the JSON has a "jobs" list for a single date (no window_days): describe each job's status plainly; if more than one ran that day, describe each separately - never assume one is "the" job. Lead with any failed/cancelled ones.
- If the JSON has "window_days" plus "failure_rate_pct" (a reliability summary): report total jobs, succeeded/failed/cancelled/in_flight counts, the failure rate, and the slowest or highest-profile jobs from the "jobs" list if present.
- If "sandbox_recognized" is explicitly false: this means the sandbox name itself doesn't exist in the data (likely a typo) - NOT that it was checked and found healthy. State this clearly and mention the suggested/valid sandbox names from the message field. Do NOT say "no breaches were found.
- Prioritized/grouped answers can run longer than a simple snapshot answer, but stay factual - no headers/markdown, plain prose paragraphs.
- If the JSON has "grouped_by": describe each entry in "buckets" (grouped by day/source/schedule/sandbox) - job_count, failure_rate_pct, avg_total_time_ms. For "grouped_by": "day", describe it as a trend over time, in date order. If a "note" field is present, mention it (it means the single-sandbox filter didn't apply to this view).
- If the JSON has "compare_previous_period": true: report current_period vs previous_period side by side using the "change" field (job_count_change, failure_rate_pct_change, avg_total_time_ms_change). If "previous_period_incomplete": true, clearly caveat that the earlier period is only partially covered by available data, using previous_period_message.
- If the JSON has an "audiences" list (audience/segment-level detail, possibly nested inside "jobs"): report audience_count, above_threshold_count, and empty_count using the stated threshold_pct. For each audience, if "segment_info_found": true, refer to it by segment_name and mention lifecycle_state; if false, refer to it by audience_id and state segment_info_message plainly rather than guessing its name or status. If "ever_crossed_threshold" is present (a single audience's history), state it plainly with times_crossed out of appearances.
"""


def narrate(user_question: str, computed_result: dict) -> str:
    user_content = (
        f"User's question: {user_question}\n\n"
        f"Computed result (JSON):\n{json.dumps(computed_result, indent=2, default=str)}"
    )

    response = client.messages.create(
        model=MODEL_ID,
        max_tokens=1024,
        system=NARRATOR_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )

    text_blocks = [block.text for block in response.content if block.type == "text"]
    return "".join(text_blocks).strip()


if __name__ == "__main__":
    from metrics.governance_standing import compute

    test_question = "How the thresshold is looking for Streaming Ingestion"
    result = compute(sandbox="xlg-prod")
    answer = narrate(test_question, result)

    print("Computed result:")
    print(json.dumps(result, indent=2, default=str))
    print("\nNarrated answer:")
    print(answer)