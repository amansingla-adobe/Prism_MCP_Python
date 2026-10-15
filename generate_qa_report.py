import json
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment

from llm.router import route
from llm.narrator import narrate
from metrics.guardrail_query import compute as compute_guardrail_query
from metrics.segment_job_query import compute as compute_segment_job_query
from metrics.segment_audience_query import compute as compute_segment_audience_query

DEFAULT_SANDBOX = "xlg-prod"

# Same dispatch pattern as test_pipeline.py/app.py. This file previously
# hard-called the guardrail compute() regardless of which metric the router
# actually matched - any segment_job_query/segment_audience_query question
# would have crashed or silently returned the wrong result. Fixed by
# dispatching through METRIC_FUNCTIONS like the other 3 entry points do.
METRIC_FUNCTIONS = {
    "guardrail_query": compute_guardrail_query,
    "segment_job_query": compute_segment_job_query,
    "segment_audience_query": compute_segment_audience_query,
}

# Union of every metric's parameter names - same list as test_pipeline.py/app.py.
# Adding a metric = add its keys here too, in all three places.
FORWARDABLE_PARAMS = (
    "guardrail_name", "window_days", "sort_by", "top_n", "compare_to_days_ago",
    "job_id", "job_date", "status", "source",
    "status_group", "group_by", "compare_previous_period",
    "audience_id", "segment_name", "threshold_pct",
)


def load_questions(path="questions.txt"):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def run_one(question: str) -> dict:
    routing = route(question)
    matched = routing.get("matched_metric")

    if not matched or matched not in METRIC_FUNCTIONS:
        return {
            "question": question, "sql_query": "(no metric matched)",
            "result_summary": routing.get("reasoning", ""),
            "final_answer": "I don't have data to answer that question yet.",
        }

    params = routing.get("parameters", {})
    call_params = {"sandbox": params.get("sandbox") or DEFAULT_SANDBOX}
    for key in FORWARDABLE_PARAMS:
        if params.get(key) is not None:
            call_params[key] = params[key]

    try:
        result = METRIC_FUNCTIONS[matched](**call_params)
    except Exception as e:
        # Surface a bad routing/param combo as a visible report row instead
        # of crashing the whole batch run partway through.
        return {
            "question": question, "sql_query": "(error)",
            "result_summary": f"matched_metric={matched}, params={call_params}",
            "final_answer": f"ERROR calling {matched}: {e}",
        }

    answer = narrate(question, result)

    return {
        "question": question,
        "sql_query": result.get("sql_query_used", ""),
        "result_summary": json.dumps(result, indent=2, default=str),
        "final_answer": answer,
    }


def build_excel(rows: list, output_path="guardrail_qa_report.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "Q&A Report"

    headers = ["Question", "SQL Query Used", "Computed Result", "Final Answer"]
    ws.append(headers)
    for col_idx in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.font = Font(bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical="top")

    for row in rows:
        ws.append([row["question"], row["sql_query"], row["result_summary"], row["final_answer"]])

    widths = {"A": 40, "B": 60, "C": 50, "D": 50}
    for col, width in widths.items():
        ws.column_dimensions[col].width = width

    for row_cells in ws.iter_rows(min_row=2):
        for cell in row_cells:
            cell.alignment = Alignment(wrap_text=True, vertical="top")

    wb.save(output_path)
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    questions = load_questions()
    print(f"Running {len(questions)} questions through the pipeline...")

    rows = []
    for i, q in enumerate(questions, 1):
        print(f"  [{i}/{len(questions)}] {q}")
        rows.append(run_one(q))

    build_excel(rows)