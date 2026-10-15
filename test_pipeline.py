import sys
import json

from llm.router import route
from llm.narrator import narrate
from metrics.guardrail_query import compute as compute_guardrail_query
from metrics.segment_job_query import compute as compute_segment_job_query
from metrics.segment_audience_query import compute as compute_segment_audience_query

METRIC_FUNCTIONS = {
    "guardrail_query": compute_guardrail_query,
    "segment_job_query": compute_segment_job_query,
    "segment_audience_query": compute_segment_audience_query,
}

DEFAULT_SANDBOX = "xlg-prod"

# Union of every metric's parameter names. Adding a metric = add its keys
# here too - forgetting this silently drops that param before it reaches
# compute() (exactly the bug that previously shipped for compare_to_days_ago
# in generate_qa_report.py).
FORWARDABLE_PARAMS = (
    "guardrail_name", "window_days", "sort_by", "top_n", "compare_to_days_ago",
    "job_id", "job_date", "status", "source",
    "status_group", "group_by", "compare_previous_period",
    "audience_id", "segment_name", "threshold_pct",
)


def run_pipeline(user_question: str):
    print(f"\nQuestion: {user_question}")

    routing_result = route(user_question)
    print("\n[Router output]")
    print(json.dumps(routing_result, indent=2))

    matched_metric = routing_result.get("matched_metric")

    if not matched_metric or matched_metric not in METRIC_FUNCTIONS:
        print("\n[No match - abstaining]")
        print("I don't have data to answer that question yet.")
        return

    params = routing_result.get("parameters", {})
    call_params = {"sandbox": params.get("sandbox") or DEFAULT_SANDBOX}
    for key in FORWARDABLE_PARAMS:
        if params.get(key) is not None:
            call_params[key] = params[key]

    computed_result = METRIC_FUNCTIONS[matched_metric](**call_params)

    print("\n[Computed result]")
    print(json.dumps(computed_result, indent=2, default=str))

    answer = narrate(user_question, computed_result)

    print("\n[Final answer]")
    print(answer)


if __name__ == "__main__":
    question = " ".join(sys.argv[1:]) or "Are there open guardrail or governance breaches?"
    run_pipeline(question)