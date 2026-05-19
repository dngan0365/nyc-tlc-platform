"""
serving/rag_app/rag_chain.py
LangChain RAG chain: natural-language → Athena SQL → grounded answer.

Architecture
------------
1. SchemaRetriever   – fetches Gold layer table schemas from Glue catalog
2. SqlGenerator      – LLM prompt: system schema context + user question → SQL
3. AthenaExecutor    – runs the SQL, returns rows as markdown table
4. AnswerSynthesiser – LLM prompt: rows + question → plain-English answer

Retry logic re-prompts the SQL generator with the Athena error message so
the model can self-correct (up to `max_sql_retries` times).
"""

from __future__ import annotations

import json
import logging
import textwrap
from dataclasses import dataclass, field
from typing import Any

import boto3
import botocore.exceptions
from anthropic import AsyncAnthropic

logger = logging.getLogger("nyc_tlc.rag_chain")

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class TableSchema:
    database: str
    table: str
    columns: list[dict[str, str]]  # [{"name": ..., "type": ..., "comment": ...}]
    partition_keys: list[str] = field(default_factory=list)

    def to_ddl(self) -> str:
        col_lines = ",\n    ".join(
            f"{c['name']} {c['type']}"
            + (f"  -- {c['comment']}" if c.get("comment") else "")
            for c in self.columns
        )
        partition_clause = ""
        if self.partition_keys:
            partition_clause = f"\nPARTITIONED BY ({', '.join(self.partition_keys)})"
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


# ---------------------------------------------------------------------------
# Schema retriever (Glue catalog)
# ---------------------------------------------------------------------------

GOLD_TABLES = [
    "fact_trips",
    "dim_location",
    "dim_datetime",
    "mart_hourly_kpi",
]


class SchemaRetriever:
    """Fetches table schemas from AWS Glue Data Catalog."""

    def __init__(self, database: str, aws_region: str) -> None:
        self._db = database
        self._glue = boto3.client("glue", region_name=aws_region)
        self._cache: dict[str, TableSchema] = {}

    def get_schemas(self, tables: list[str] | None = None) -> list[TableSchema]:
        targets = tables or GOLD_TABLES
        schemas: list[TableSchema] = []
        for table_name in targets:
            if table_name not in self._cache:
                self._cache[table_name] = self._fetch(table_name)
            schemas.append(self._cache[table_name])
        return schemas

    def _fetch(self, table_name: str) -> TableSchema:
        try:
            resp = self._glue.get_table(DatabaseName=self._db, Name=table_name)
        except botocore.exceptions.ClientError as exc:
            logger.warning("Glue table %s.%s not found: %s", self._db, table_name, exc)
            return TableSchema(database=self._db, table=table_name, columns=[])

        sd = resp["Table"]["StorageDescriptor"]
        columns = [
            {
                "name": c["Name"],
                "type": c["Type"],
                "comment": c.get("Comment", ""),
            }
            for c in sd.get("Columns", [])
        ]
        partition_keys = [
            p["Name"] for p in resp["Table"].get("PartitionKeys", [])
        ]
        return TableSchema(
            database=self._db,
            table=table_name,
            columns=columns,
            partition_keys=partition_keys,
        )


# ---------------------------------------------------------------------------
# Athena executor
# ---------------------------------------------------------------------------

class AthenaExecutor:
    """Submits SQL to Athena and waits for results."""

    _TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}

    def __init__(
        self,
        workgroup: str,
        results_location: str,
        aws_region: str,
    ) -> None:
        self._wg = workgroup
        self._results_location = results_location
        self._athena = boto3.client("athena", region_name=aws_region)

    def run(self, sql: str, max_rows: int = 50) -> tuple[list[dict[str, Any]], str]:
        """
        Execute *sql* and return (rows, execution_id).
        Raises RuntimeError with the Athena error message on failure.
        """
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
        import time
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
            PaginationConfig={"MaxItems": max_rows + 1},  # +1 for header row
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
    body = "\n".join(" | ".join(str(r.get(h, "")) for h in headers) for r in rows)
    return f"| {header_line} |\n| {sep} |\n" + "\n".join(
        f"| {' | '.join(str(r.get(h, '')) for h in headers)} |" for r in rows
    )


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_SQL_SYSTEM = textwrap.dedent("""\
    You are an expert data analyst. You have access to the following Athena tables
    (AWS Glue / Delta Lake, Presto SQL dialect):

    {schema_ddl}

    Rules:
    - Write valid Presto/Athena SQL only.
    - Always qualify table names as database.table (e.g. nyc_tlc_gold.fact_trips).
    - Filter on partition columns (pickup_date, year, month) whenever possible.
    - NEVER use SELECT *; list columns explicitly.
    - Limit result sets to {max_rows} rows with a LIMIT clause.
    - Return ONLY the raw SQL query, no explanation, no markdown fences.
""")

_SQL_RETRY_SUFFIX = textwrap.dedent("""\

    Your previous SQL attempt failed with this Athena error:
    ---
    {error}
    ---
    Rewrite the SQL to fix the error. Return ONLY the corrected SQL.
""")

_ANSWER_SYSTEM = textwrap.dedent("""\
    You are a data analyst assistant. Given a user question and query results
    (as a markdown table), write a concise, accurate, plain-English answer.
    - Ground every claim in the data provided; do not hallucinate numbers.
    - If the table is empty, say so and suggest why the query might have returned nothing.
    - Keep the answer under 150 words.
""")


# ---------------------------------------------------------------------------
# Main RAG chain
# ---------------------------------------------------------------------------

class NycTlcRagChain:
    """
    Two-stage LLM chain:
      Stage 1 — generate + execute Athena SQL (with self-correcting retries)
      Stage 2 — synthesise a grounded plain-English answer from the result rows
    """

    def __init__(
        self,
        athena_database: str,
        athena_workgroup: str,
        athena_results_location: str,
        aws_region: str,
        model_id: str = "claude-sonnet-4-20250514",
        max_sql_retries: int = 2,
    ) -> None:
        self._model_id = model_id
        self._max_retries = max_sql_retries
        self._schema_retriever = SchemaRetriever(
            database=athena_database, aws_region=aws_region
        )
        self._executor = AthenaExecutor(
            workgroup=athena_workgroup,
            results_location=athena_results_location,
            aws_region=aws_region,
        )
        self._client = AsyncAnthropic()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def ainvoke(self, question: str, max_rows: int = 50) -> RagAnswer:
        schemas = self._schema_retriever.get_schemas()
        schema_ddl = "\n\n".join(s.to_ddl() for s in schemas)

        sql, rows = await self._generate_and_execute(
            question=question,
            schema_ddl=schema_ddl,
            max_rows=max_rows,
        )
        answer = await self._synthesise_answer(
            question=question,
            rows=rows,
        )
        return RagAnswer(
            answer=answer,
            sql=sql,
            rows_returned=len(rows),
            raw_rows=rows,
        )

    # ------------------------------------------------------------------
    # Stage 1: SQL generation with self-correcting retry loop
    # ------------------------------------------------------------------

    async def _generate_and_execute(
        self,
        question: str,
        schema_ddl: str,
        max_rows: int,
    ) -> tuple[str, list[dict[str, Any]]]:
        system = _SQL_SYSTEM.format(schema_ddl=schema_ddl, max_rows=max_rows)
        messages: list[dict] = [{"role": "user", "content": question}]
        last_error: str | None = None

        for attempt in range(self._max_retries + 1):
            if last_error and attempt > 0:
                # Append the error so the model can self-correct
                retry_note = _SQL_RETRY_SUFFIX.format(error=last_error)
                messages.append({"role": "user", "content": retry_note})

            sql = await self._llm(system=system, messages=messages)
            sql = sql.strip().rstrip(";")

            logger.info("SQL attempt %d:\n%s", attempt + 1, sql)

            try:
                rows, exec_id = self._executor.run(sql, max_rows=max_rows)
                logger.info(
                    "Athena query succeeded: exec_id=%s rows=%d", exec_id, len(rows)
                )
                return sql, rows
            except RuntimeError as exc:
                last_error = str(exc)
                logger.warning("SQL attempt %d failed: %s", attempt + 1, last_error)
                # Push the assistant's previous SQL into history for context
                messages.append({"role": "assistant", "content": sql})

        raise RuntimeError(
            f"SQL generation failed after {self._max_retries + 1} attempts. "
            f"Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Stage 2: Answer synthesis
    # ------------------------------------------------------------------

    async def _synthesise_answer(
        self,
        question: str,
        rows: list[dict[str, Any]],
    ) -> str:
        markdown_table = rows_to_markdown(rows)
        user_content = (
            f"Question: {question}\n\nQuery results:\n{markdown_table}"
        )
        return await self._llm(
            system=_ANSWER_SYSTEM,
            messages=[{"role": "user", "content": user_content}],
        )

    # ------------------------------------------------------------------
    # LLM helper
    # ------------------------------------------------------------------

    async def _llm(
        self,
        system: str,
        messages: list[dict],
        max_tokens: int = 1024,
    ) -> str:
        response = await self._client.messages.create(
            model=self._model_id,
            max_tokens=max_tokens,
            system=system,
            messages=messages,
        )
        return response.content[0].text