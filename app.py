from flask import Flask, request, jsonify, render_template
from llm.router import route
from llm.narrator import narrate
from metrics.guardrail_query import compute as compute_guardrail_query
from metrics.segment_job_query import compute as compute_segment_job_query
from metrics.segment_audience_query import compute as compute_segment_audience_query

app = Flask(__name__)
DEFAULT_SANDBOX = "xlg-prod"

METRIC_FUNCTIONS = {
    "guardrail_query": compute_guardrail_query,
    "segment_job_query": compute_segment_job_query,
    "segment_audience_query": compute_segment_audience_query,
}

# Union of every metric's parameter names - same list as test_pipeline.py.
# Adding a metric = add its keys here too, in both places.
FORWARDABLE_PARAMS = (
    "guardrail_name", "window_days", "sort_by", "top_n", "compare_to_days_ago",
    "job_id", "job_date", "status", "source",
    "status_group", "group_by", "compare_previous_period",
    "audience_id", "segment_name", "threshold_pct",
)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/ask", methods=["POST"])
def ask():
    data = request.get_json()
    question = (data.get("question") or "").strip()

    if not question:
        return jsonify({"answer": "Please type a question.", "sql_query": None})

    routing = route(question)
    matched_metric = routing.get("matched_metric")

    if not matched_metric or matched_metric not in METRIC_FUNCTIONS:
        return jsonify({"answer": "I don't have data to answer that question yet.", "sql_query": None})

    params = routing.get("parameters", {})
    call_params = {"sandbox": params.get("sandbox") or DEFAULT_SANDBOX}
    for key in FORWARDABLE_PARAMS:
        if params.get(key) is not None:
            call_params[key] = params[key]

    result = METRIC_FUNCTIONS[matched_metric](**call_params)
    answer = narrate(question, result)

    return jsonify({
        "answer": answer,
        "sql_query": result.get("sql_query_used", ""),
        "status": result.get("overall_status") or result.get("status"),
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)