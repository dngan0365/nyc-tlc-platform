"""
serving/rag_app/eval_harness.py
Evaluation harness for the NYC TLC RAG chain.

Runs 5 representative Q&A test cases against the live chain, scores each
on answer correctness (keyword-based precision), SQL validity, and latency,
then logs every artefact to an MLflow experiment.

Usage
-----
    # Against a running API:
    python eval_harness.py --mode api --api-url http://localhost:8000

    # Direct chain invocation (needs AWS creds):
    python eval_harness.py --mode chain

MLflow artefacts logged per run
--------------------------------
- params:   model_id, athena_database, eval_mode, timestamp
- metrics:  precision_mean, latency_p50_ms, latency_p95_ms, pass_rate
- per-case: precision_<n>, latency_ms_<n>, sql_valid_<n>
- artefact: eval_results.json  (full case-by-case detail)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import statistics
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import mlflow
import mlflow.entities

from rag_chain import NycTlcRagChain, RagAnswer

logger = logging.getLogger("nyc_tlc.eval")
logging.basicConfig(level="INFO", format="%(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

@dataclass
class EvalCase:
    """A single evaluation example."""
    id: str
    question: str
    # Keywords that MUST appear in a correct answer (case-insensitive)
    required_keywords: list[str]
    # Keywords that should NOT appear (hallucination guard)
    forbidden_keywords: list[str] = field(default_factory=list)
    description: str = ""


EVAL_CASES: list[EvalCase] = [
    EvalCase(
        id="avg_fare_jan2023",
        question="What was the average fare amount for yellow cab trips in January 2023?",
        required_keywords=["average", "fare", "january", "2023"],
        description="Basic aggregation on fact_trips with date partition filter.",
    ),
    EvalCase(
        id="top_pickup_zone",
        question="Which pickup zone had the highest number of trips in Q1 2023?",
        required_keywords=["zone", "trips", "2023"],
        forbidden_keywords=["i don't know", "cannot determine"],
        description="Join fact_trips → dim_location, group by zone, sort desc.",
    ),
    EvalCase(
        id="payment_type_share",
        question=(
            "What percentage of yellow cab trips in 2023 were paid by credit card "
            "versus cash?"
        ),
        required_keywords=["credit card", "cash", "percent"],
        description="Payment type breakdown — tests dim join and ratio calculation.",
    ),
    EvalCase(
        id="hourly_pattern",
        question=(
            "Show me the average number of trips per hour of the day across all of 2023 "
            "for Manhattan pickups."
        ),
        required_keywords=["hour", "manhattan", "trips"],
        description="Uses mart_hourly_kpi or window function on fact_trips + dim_location.",
    ),
    EvalCase(
        id="empty_result_graceful",
        question=(
            "How many trips were recorded in the nyc_tlc_gold database on 2099-01-01?"
        ),
        required_keywords=["no", "zero", "0", "empty", "returned", "none"],
        forbidden_keywords=["error", "exception"],
        description="Empty result set — verifies the chain handles zero rows gracefully.",
    ),
]

# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class CaseResult:
    case_id: str
    question: str
    answer: str
    sql: str
    rows_returned: int
    latency_ms: float
    precision: float        # fraction of required_keywords present in answer
    sql_valid: bool         # True if SQL ran without error
    passed: bool            # precision == 1.0 and sql_valid and no forbidden hits
    forbidden_hit: bool     # True if a forbidden keyword was found
    error: str | None = None


@dataclass
class EvalRun:
    run_id: str
    timestamp: str
    model_id: str
    athena_database: str
    eval_mode: str
    cases: list[CaseResult]

    @property
    def precision_mean(self) -> float:
        return statistics.mean(c.precision for c in self.cases) if self.cases else 0.0

    @property
    def pass_rate(self) -> float:
        return sum(c.passed for c in self.cases) / len(self.cases) if self.cases else 0.0

    @property
    def latency_p50_ms(self) -> float:
        lats = sorted(c.latency_ms for c in self.cases)
        return lats[len(lats) // 2] if lats else 0.0

    @property
    def latency_p95_ms(self) -> float:
        lats = sorted(c.latency_ms for c in self.cases)
        idx = int(len(lats) * 0.95)
        return lats[min(idx, len(lats) - 1)] if lats else 0.0


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def score_case(case: EvalCase, answer: str, sql: str, sql_valid: bool) -> tuple[float, bool, bool]:
    """
    Returns (precision, passed, forbidden_hit).
    precision = fraction of required_keywords found in answer (case-insensitive).
    """
    lower_answer = answer.lower()
    hits = sum(kw.lower() in lower_answer for kw in case.required_keywords)
    precision = hits / len(case.required_keywords) if case.required_keywords else 1.0

    forbidden_hit = any(kw.lower() in lower_answer for kw in case.forbidden_keywords)
    passed = precision == 1.0 and sql_valid and not forbidden_hit
    return precision, passed, forbidden_hit


# ---------------------------------------------------------------------------
# Chain runner (direct)
# ---------------------------------------------------------------------------

async def run_chain_direct(
    cases: list[EvalCase],
    chain: NycTlcRagChain,
) -> list[CaseResult]:
    results: list[CaseResult] = []
    for case in cases:
        t0 = time.perf_counter()
        error: str | None = None
        answer = ""
        sql = ""
        rows_returned = 0
        sql_valid = False

        try:
            rag: RagAnswer = await chain.ainvoke(question=case.question)
            answer = rag.answer
            sql = rag.sql
            rows_returned = rag.rows_returned
            sql_valid = True
        except Exception as exc:
            error = str(exc)
            answer = f"ERROR: {exc}"
            logger.error("Case %s failed: %s", case.id, exc)

        latency_ms = (time.perf_counter() - t0) * 1000
        precision, passed, forbidden_hit = score_case(case, answer, sql, sql_valid)

        results.append(
            CaseResult(
                case_id=case.id,
                question=case.question,
                answer=answer,
                sql=sql,
                rows_returned=rows_returned,
                latency_ms=round(latency_ms, 1),
                precision=round(precision, 3),
                sql_valid=sql_valid,
                passed=passed,
                forbidden_hit=forbidden_hit,
                error=error,
            )
        )
    return results


# ---------------------------------------------------------------------------
# API runner (hits the FastAPI /query endpoint)
# ---------------------------------------------------------------------------

async def run_chain_api(
    cases: list[EvalCase],
    api_url: str,
) -> list[CaseResult]:
    results: list[CaseResult] = []
    async with httpx.AsyncClient(base_url=api_url, timeout=120.0) as client:
        for case in cases:
            t0 = time.perf_counter()
            error: str | None = None
            answer = ""
            sql = ""
            rows_returned = 0
            sql_valid = False

            try:
                resp = await client.post(
                    "/query",
                    json={
                        "question": case.question,
                        "max_rows": 50,
                        "include_sql": True,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                answer = data["answer"]
                sql = data.get("sql") or ""
                rows_returned = data["rows_returned"]
                sql_valid = True
            except Exception as exc:
                error = str(exc)
                answer = f"ERROR: {exc}"
                logger.error("Case %s failed via API: %s", case.id, exc)

            latency_ms = (time.perf_counter() - t0) * 1000
            precision, passed, forbidden_hit = score_case(case, answer, sql, sql_valid)

            results.append(
                CaseResult(
                    case_id=case.id,
                    question=case.question,
                    answer=answer,
                    sql=sql,
                    rows_returned=rows_returned,
                    latency_ms=round(latency_ms, 1),
                    precision=round(precision, 3),
                    sql_valid=sql_valid,
                    passed=passed,
                    forbidden_hit=forbidden_hit,
                    error=error,
                )
            )
    return results


# ---------------------------------------------------------------------------
# MLflow logging
# ---------------------------------------------------------------------------

def log_to_mlflow(run: EvalRun) -> str:
    """Log the eval run to MLflow and return the run URL."""
    experiment_name = "nyc-tlc-rag-eval"
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"eval_{run.timestamp}") as mlflow_run:
        # Run-level params
        mlflow.log_params(
            {
                "model_id": run.model_id,
                "athena_database": run.athena_database,
                "eval_mode": run.eval_mode,
                "timestamp": run.timestamp,
                "n_cases": len(run.cases),
            }
        )

        # Aggregate metrics
        mlflow.log_metrics(
            {
                "precision_mean": round(run.precision_mean, 4),
                "pass_rate": round(run.pass_rate, 4),
                "latency_p50_ms": round(run.latency_p50_ms, 1),
                "latency_p95_ms": round(run.latency_p95_ms, 1),
            }
        )

        # Per-case metrics
        for i, case_result in enumerate(run.cases):
            mlflow.log_metrics(
                {
                    f"precision_{i}_{case_result.case_id}": case_result.precision,
                    f"latency_ms_{i}_{case_result.case_id}": case_result.latency_ms,
                    f"sql_valid_{i}_{case_result.case_id}": int(case_result.sql_valid),
                    f"passed_{i}_{case_result.case_id}": int(case_result.passed),
                }
            )

        # Full results JSON as artefact
        results_path = "/tmp/eval_results.json"
        with open(results_path, "w") as f:
            json.dump(
                {
                    "run_id": run.run_id,
                    "timestamp": run.timestamp,
                    "summary": {
                        "precision_mean": run.precision_mean,
                        "pass_rate": run.pass_rate,
                        "latency_p50_ms": run.latency_p50_ms,
                        "latency_p95_ms": run.latency_p95_ms,
                    },
                    "cases": [asdict(c) for c in run.cases],
                },
                f,
                indent=2,
            )
        mlflow.log_artifact(results_path, artifact_path="eval")

        run_url = (
            f"{mlflow.get_tracking_uri()}/#/experiments/"
            f"{mlflow_run.info.experiment_id}/runs/{mlflow_run.info.run_id}"
        )
        return run_url


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _print_summary(run: EvalRun) -> None:
    print("\n" + "=" * 60)
    print(f"  NYC TLC RAG Eval — {run.timestamp}")
    print("=" * 60)
    print(f"  Cases:        {len(run.cases)}")
    print(f"  Pass rate:    {run.pass_rate:.0%}  ({sum(c.passed for c in run.cases)}/{len(run.cases)})")
    print(f"  Precision:    {run.precision_mean:.3f}")
    print(f"  Latency p50:  {run.latency_p50_ms:.0f} ms")
    print(f"  Latency p95:  {run.latency_p95_ms:.0f} ms")
    print("-" * 60)
    for c in run.cases:
        icon = "✓" if c.passed else "✗"
        print(f"  {icon} [{c.case_id}]  precision={c.precision:.2f}  latency={c.latency_ms:.0f}ms")
        if not c.passed:
            print(f"      answer: {c.answer[:120]}")
    print("=" * 60 + "\n")


async def _main(args: argparse.Namespace) -> None:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    model_id = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
    athena_db = os.getenv("ATHENA_DATABASE", "nyc_tlc_gold")

    if args.mode == "api":
        case_results = await run_chain_api(EVAL_CASES, api_url=args.api_url)
    else:
        chain = NycTlcRagChain(
            athena_database=athena_db,
            athena_workgroup=os.environ["ATHENA_WORKGROUP"],
            athena_results_location=os.environ["ATHENA_RESULTS_BUCKET"],
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            model_id=model_id,
        )
        case_results = await run_chain_direct(EVAL_CASES, chain=chain)

    eval_run = EvalRun(
        run_id=run_id,
        timestamp=run_id,
        model_id=model_id,
        athena_database=athena_db,
        eval_mode=args.mode,
        cases=case_results,
    )

    _print_summary(eval_run)

    if not args.no_mlflow:
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
        run_url = log_to_mlflow(eval_run)
        print(f"MLflow run logged → {run_url}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC TLC RAG evaluation harness")
    parser.add_argument(
        "--mode",
        choices=["chain", "api"],
        default="api",
        help="'chain' = invoke RagChain directly; 'api' = hit FastAPI endpoint",
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="Base URL of the FastAPI app (used when --mode=api)",
    )
    parser.add_argument(
        "--no-mlflow",
        action="store_true",
        help="Skip MLflow logging (useful for quick local runs)",
    )
    asyncio.run(_main(parser.parse_args()))