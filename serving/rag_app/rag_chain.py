"""
serving/rag_app/rag_chain.py
LangChain LCEL RAG chain: natural-language → Athena SQL → grounded answer.

Stack
-----
- LLM backend : OpenAI (gpt-4o by default) via langchain-openai
- Chain style  : LangChain LCEL (pipe operator |) with ChatPromptTemplate +
                 StrOutputParser
- Async        : uses LangChain's ainvoke throughout; Athena I/O offloaded to
                 asyncio.to_thread() so the event loop is never blocked
- MLflow       : every ainvoke() call is a tracked run

Architecture
------------
1. SchemaRetriever   – fetches Gold + Marts layer schemas from Glue catalog
                       (supports multiple databases in one retriever)
2. sql_chain         – LCEL: schema context + user question → SQL
3. AthenaExecutor    – runs the SQL, returns rows + markdown table
4. answer_chain      – LCEL: rows + question → plain-English answer + follow-ups

Retry logic re-prompts the SQL chain with the Athena error message so the
model can self-correct (up to `max_sql_retries` times).

Bugs fixed vs. previous version
---------------------------------
1. SchemaRetriever now accepts multiple databases so mart tables
   (nyc_tlc_gold_marts) are fetched alongside gold tables (nyc_tlc_gold).
2. Partition-key columns are now included in to_ddl() — they were previously
   omitted because they live under PartitionKeys, not StorageDescriptor.Columns.
3. _ANSWER_SYSTEM no longer contains the stray "Return ONLY the raw SQL query"
   rule that was copy-pasted from _SQL_SYSTEM.
4. _SQL_SYSTEM partition guidance now references the actual column names used
   in the mart and gold tables (pickup_year, pickup_month as VARCHAR).
5. AthenaExecutor.run() is now called via asyncio.to_thread() so it does not
   block the event loop.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import textwrap
import time
from dataclasses import dataclass, field
from typing import Any

import boto3
import botocore.exceptions
import mlflow
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI

logger = logging.getLogger("nyc_tlc.rag_chain")

# ---------------------------------------------------------------------------
# MLflow setup
# ---------------------------------------------------------------------------

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT   = os.getenv("MLFLOW_EXPERIMENT_NAME", "nyc-tlc-rag")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

try:
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
except Exception as exc:
    logger.warning("MLflow experiment setup failed (will retry per-run): %s", exc)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class TableSchema:
    database: str
    table: str
    columns: list[dict[str, str]]
    # FIX #2: partition_keys now populated from Glue's PartitionKeys list
    # and included in to_ddl() so the LLM sees them.
    partition_keys: list[dict[str, str]] = field(default_factory=list)

    def to_ddl(self) -> str:
        def _col_line(c: dict[str, str]) -> str:
            line = f"{c['name']} {c['type']}"
            if c.get("comment"):
                line += f"  -- {c['comment']}"
            return line

        col_lines = ",\n    ".join(_col_line(c) for c in self.columns)

        partition_clause = ""
        if self.partition_keys:
            pk_lines = ", ".join(
                f"{p['name']} {p['type']}" for p in self.partition_keys
            )
            partition_clause = f"\nPARTITIONED BY ({pk_lines})"

        return (
            f"-- {self.database}.{self.table}\n"
            f"CREATE EXTERNAL TABLE {self.database}.{self.table} (\n"
            f"    {col_lines}\n"
            f"){partition_clause};"
        )


@dataclass
class RagAnswer:
    answer: str
    sql: str
    rows_returned: int
    raw_rows: list[dict[str, Any]]
    sql_attempts: int = 1
    latency_seconds: float = 0.0
    follow_up_questions: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Schema retriever (Glue catalog)
# ---------------------------------------------------------------------------

# FIX #1: mart tables live in nyc_tlc_gold_marts, not nyc_tlc_gold.
# SchemaRetriever now accepts a dict mapping database → list[table].
GOLD_TABLES: dict[str, list[str]] = {
    "nyc_tlc_gold_marts": [
        "fact_trips",
        "dim_location",
        "dim_datetime",
        "mart_hourly_kpi",
    ],
}


class SchemaRetriever:
    """Fetches table schemas from AWS Glue Data Catalog.

    Accepts a mapping of {database: [table, ...]} so schemas can be retrieved
    across multiple Glue databases in a single call.
    """

    def __init__(self, aws_region: str) -> None:
        self._glue = boto3.client("glue", region_name=aws_region)
        self._cache: dict[tuple[str, str], TableSchema] = {}

    def get_schemas(
        self,
        tables: dict[str, list[str]] | None = None,
    ) -> list[TableSchema]:
        targets = tables or GOLD_TABLES
        schemas: list[TableSchema] = []
        for db, table_names in targets.items():
            for table_name in table_names:
                key = (db, table_name)
                if key not in self._cache:
                    self._cache[key] = self._fetch(db, table_name)
                schemas.append(self._cache[key])
        return schemas

    def _fetch(self, database: str, table_name: str) -> TableSchema:
        try:
            resp = self._glue.get_table(DatabaseName=database, Name=table_name)
        except botocore.exceptions.ClientError as exc:
            logger.warning(
                "Glue table %s.%s not found: %s", database, table_name, exc
            )
            return TableSchema(database=database, table=table_name, columns=[])

        sd = resp["Table"]["StorageDescriptor"]

        # Regular (non-partition) columns
        columns = [
            {
                "name":    c["Name"],
                "type":    c["Type"],
                "comment": c.get("Comment", ""),
            }
            for c in sd.get("Columns", [])
        ]

        # FIX #2: include partition key columns so to_ddl() reflects reality.
        # Glue stores these separately under Table.PartitionKeys.
        partition_keys = [
            {
                "name":    p["Name"],
                "type":    p["Type"],
                "comment": p.get("Comment", ""),
            }
            for p in resp["Table"].get("PartitionKeys", [])
        ]

        return TableSchema(
            database=database,
            table=table_name,
            columns=columns,
            partition_keys=partition_keys,
        )


# ---------------------------------------------------------------------------
# Athena executor
# ---------------------------------------------------------------------------

class AthenaExecutor:
    """Submits SQL to Athena and waits for results (blocking I/O)."""

    _TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}

    def __init__(self, workgroup: str, results_location: str, aws_region: str) -> None:
        self._wg = workgroup
        self._results_location = results_location
        self._athena = boto3.client("athena", region_name=aws_region)

    def run(self, sql: str, max_rows: int = 50) -> tuple[list[dict[str, Any]], str]:
        """Execute *sql* synchronously. Raises RuntimeError on failure."""
        start_resp = self._athena.start_query_execution(
            QueryString=sql,
            WorkGroup=self._wg,
            ResultConfiguration={"OutputLocation": self._results_location},
        )
        exec_id: str = start_resp["QueryExecutionId"]
        self._wait(exec_id)
        rows = self._fetch_rows(exec_id, max_rows)
        return rows, exec_id

    def _wait(self, exec_id: str) -> None:
        while True:
            resp = self._athena.get_query_execution(QueryExecutionId=exec_id)
            state = resp["QueryExecution"]["Status"]["State"]
            if state in self._TERMINAL:
                if state != "SUCCEEDED":
                    reason = (
                        resp["QueryExecution"]["Status"]
                        .get("StateChangeReason", "Unknown Athena error")
                    )
                    raise RuntimeError(f"Athena query {state}: {reason}")
                return
            time.sleep(1.5)

    def _fetch_rows(self, exec_id: str, max_rows: int) -> list[dict[str, Any]]:
        paginator = self._athena.get_paginator("get_query_results")
        pages = paginator.paginate(
            QueryExecutionId=exec_id,
            PaginationConfig={"MaxItems": max_rows + 1},
        )
        rows: list[dict[str, Any]] = []
        headers: list[str] = []
        for page in pages:
            result_rows = page["ResultSet"]["Rows"]
            if not headers:
                headers = [c["VarCharValue"] for c in result_rows[0]["Data"]]
                result_rows = result_rows[1:]
            for row in result_rows:
                rows.append(
                    {
                        headers[i]: cell.get("VarCharValue", "")
                        for i, cell in enumerate(row["Data"])
                    }
                )
        return rows[:max_rows]


def rows_to_markdown(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "_No rows returned._"
    headers = list(rows[0].keys())
    sep = " | ".join("---" for _ in headers)
    header_line = " | ".join(headers)
    return f"| {header_line} |\n| {sep} |\n" + "\n".join(
        f"| {' | '.join(str(r.get(h, '')) for h in headers)} |" for r in rows
    )


# ---------------------------------------------------------------------------
# LangChain LCEL prompts & chains
# ---------------------------------------------------------------------------

# FIX #4: partition guidance now matches the actual column names in the Glue
# schemas (pickup_year / pickup_month as VARCHAR on hourly_kpis; pickup_date
# as DATE on mart tables).
_SQL_SYSTEM = textwrap.dedent("""\
    You are an expert data analyst. You have access to the following Athena tables
    (AWS Glue / Delta Lake, Presto SQL dialect):

    {schema_ddl}

    Rules:
    - Write valid Presto/Athena SQL only.
    - Always qualify table names as database.table
      (e.g. nyc_tlc_gold_marts.fact_trips).
    - Partition columns vary by table:
        * mart_hourly_kpi / fact_trips: filter on pickup_date (DATE)
          e.g. WHERE pickup_date >= DATE '2024-01-01'
        * hourly_kpis (nyc_tlc_gold): partitioned by pickup_year VARCHAR and
          pickup_month VARCHAR — cast when comparing integers:
          WHERE pickup_year = '2024' AND pickup_month = '01'
          or    CAST(pickup_year AS INTEGER) = 2024
      Always apply a partition filter when the question implies a time range.
    - NEVER use SELECT *; list columns explicitly.
    - Limit result sets to {max_rows} rows with a LIMIT clause.
    - Return ONLY the raw SQL query — no explanation, no markdown fences.
""")

_SQL_RETRY_SUFFIX = textwrap.dedent("""\

    Your previous SQL attempt failed with this Athena error:
    ---
    {error}
    ---
    Rewrite the SQL to fix the error. Return ONLY the corrected SQL.
""")

# FIX #3: removed the stray "Return ONLY the raw SQL query" rule that was
# copy-pasted from _SQL_SYSTEM into _ANSWER_SYSTEM.
_ANSWER_SYSTEM = textwrap.dedent("""\
    You are a data analyst assistant. Given a user question and query results
    (as a markdown table), write a concise, accurate, plain-English answer AND
    suggest 3 relevant follow-up questions the user might want to ask next.

    Rules for the answer:
    - Ground every claim in the data provided; do not hallucinate numbers.
    - If the table is empty, say so and suggest why the query might have
      returned nothing.
    - Keep the answer under 150 words.

    Always respond in exactly this format (no extra text outside the tags):

    <answer>
    Your plain-English answer here.
    </answer>
    <follow_ups>
    1. First follow-up question?
    2. Second follow-up question?
    3. Third follow-up question?
    </follow_ups>
""")


def _build_sql_chain(llm: ChatOpenAI) -> Any:
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", _SQL_SYSTEM),
            ("human", "{question}{error_suffix}"),
        ]
    )
    return prompt | llm | StrOutputParser()


def _build_answer_chain(llm: ChatOpenAI) -> Any:
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", _ANSWER_SYSTEM),
            ("human", "Question: {question}\n\nQuery results:\n{markdown_table}"),
        ]
    )
    return prompt | llm | StrOutputParser()


def _parse_answer_response(raw: str) -> tuple[str, list[str]]:
    answer_match = re.search(r"<answer>(.*?)</answer>", raw, re.DOTALL)
    fu_match     = re.search(r"<follow_ups>(.*?)</follow_ups>", raw, re.DOTALL)

    answer    = answer_match.group(1).strip() if answer_match else raw.strip()
    follow_ups: list[str] = []
    if fu_match:
        for line in fu_match.group(1).strip().splitlines():
            line = re.sub(r"^\s*\d+[.)]\s*", "", line).strip()
            if line:
                follow_ups.append(line)

    return answer, follow_ups[:3]


# ---------------------------------------------------------------------------
# Main RAG chain
# ---------------------------------------------------------------------------

class NycTlcRagChain:
    """
    Two-stage LangChain LCEL RAG chain.

    Stage 1 — sql_chain generates + AthenaExecutor runs SQL (with retries).
    Stage 2 — answer_chain synthesises answer + 3 follow-up questions.

    Every call is tracked in MLflow under the "nyc-tlc-rag" experiment.
    """

    def __init__(
        self,
        athena_workgroup: str,
        athena_results_location: str,
        aws_region: str,
        model_id: str = "gpt-4o",
        max_sql_retries: int = 2,
        temperature: float = 0.0,
        # Optional override: pass your own {database: [tables]} mapping.
        glue_tables: dict[str, list[str]] | None = None,
    ) -> None:
        self._model_id   = model_id
        self._max_retries = max_sql_retries
        self._glue_tables = glue_tables or GOLD_TABLES

        # FIX #1: SchemaRetriever no longer takes a single database arg.
        self._schema_retriever = SchemaRetriever(aws_region=aws_region)
        self._executor = AthenaExecutor(
            workgroup=athena_workgroup,
            results_location=athena_results_location,
            aws_region=aws_region,
        )

        llm = ChatOpenAI(model=model_id, temperature=temperature)
        self._sql_chain    = _build_sql_chain(llm)
        self._answer_chain = _build_answer_chain(llm)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def ainvoke(self, question: str, max_rows: int = 50) -> RagAnswer:
        t0 = time.perf_counter()

        try:
            mlflow.set_experiment(MLFLOW_EXPERIMENT)
        except Exception:
            pass

        with mlflow.start_run():
            mlflow.log_params({
                "model_id":        self._model_id,
                "max_rows":        max_rows,
                "max_sql_retries": self._max_retries,
                "question":        question[:250],
            })

            try:
                schemas   = self._schema_retriever.get_schemas(self._glue_tables)
                schema_ddl = "\n\n".join(s.to_ddl() for s in schemas)

                sql, rows, sql_attempts = await self._generate_and_execute(
                    question=question,
                    schema_ddl=schema_ddl,
                    max_rows=max_rows,
                )
                answer, follow_ups = await self._synthesise_answer(
                    question=question,
                    rows=rows,
                )

                latency = time.perf_counter() - t0
                mlflow.log_metrics({
                    "rows_returned":   float(len(rows)),
                    "sql_attempts":    float(sql_attempts),
                    "latency_seconds": round(latency, 3),
                    "success":         1.0,
                })
                mlflow.set_tags({"sql": sql[:500]})

                return RagAnswer(
                    answer=answer,
                    sql=sql,
                    rows_returned=len(rows),
                    raw_rows=rows,
                    sql_attempts=sql_attempts,
                    latency_seconds=round(latency, 3),
                    follow_up_questions=follow_ups,
                )

            except Exception as exc:
                latency = time.perf_counter() - t0
                mlflow.log_metrics({"latency_seconds": round(latency, 3), "success": 0.0})
                mlflow.set_tag("error", str(exc)[:500])
                logger.error("RAG chain failed: %s", exc, exc_info=True)
                raise

    # ------------------------------------------------------------------
    # Stage 1: SQL generation with self-correcting retry loop
    # ------------------------------------------------------------------

    async def _generate_and_execute(
        self,
        question: str,
        schema_ddl: str,
        max_rows: int,
    ) -> tuple[str, list[dict[str, Any]], int]:
        last_error: str | None = None

        for attempt in range(self._max_retries + 1):
            error_suffix = (
                _SQL_RETRY_SUFFIX.format(error=last_error)
                if last_error and attempt > 0
                else ""
            )

            sql: str = await self._sql_chain.ainvoke({
                "schema_ddl":   schema_ddl,
                "max_rows":     max_rows,
                "question":     question,
                "error_suffix": error_suffix,
            })
            sql = sql.strip().rstrip(";")
            logger.info("SQL attempt %d:\n%s", attempt + 1, sql)

            try:
                # FIX #5: run blocking Athena I/O in a thread pool so the
                # async event loop is never blocked.
                rows, exec_id = await asyncio.to_thread(
                    self._executor.run, sql, max_rows
                )
                logger.info(
                    "Athena query succeeded: exec_id=%s rows=%d", exec_id, len(rows)
                )
                return sql, rows, attempt + 1
            except RuntimeError as exc:
                last_error = str(exc)
                logger.warning("SQL attempt %d failed: %s", attempt + 1, last_error)

        raise RuntimeError(
            f"SQL generation failed after {self._max_retries + 1} attempts. "
            f"Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Stage 2: Answer synthesis + follow-up questions
    # ------------------------------------------------------------------

    async def _synthesise_answer(
        self,
        question: str,
        rows: list[dict[str, Any]],
    ) -> tuple[str, list[str]]:
        markdown_table = rows_to_markdown(rows)
        raw: str = await self._answer_chain.ainvoke({
            "question":       question,
            "markdown_table": markdown_table,
        })
        return _parse_answer_response(raw)