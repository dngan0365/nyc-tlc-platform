"""
serving/rag_app/streamlit_app.py
Streamlit chat interface for the NYC TLC RAG API.
Uses OpenAI + LangChain for the RAG chain (text-to-SQL → Athena → answer).

Layout
------
Left sidebar   — app branding + settings (model, temp, max rows)
Main area      — chat window (user bubbles right, assistant left)
Right panel    — Top-5 KPI metric cards from the last RAG result

Run
---
    streamlit run streamlit_app.py

Environment variables
---------------------
    OPENAI_API_KEY          required
    ATHENA_DATABASE         default: nyc_tlc_gold
    ATHENA_WORKGROUP        default: nyc-tlc-dev
    ATHENA_RESULTS_BUCKET   default: s3://nyc-tlc-athena-results-dev/
    AWS_REGION              default: us-east-1
"""

from __future__ import annotations

import os
import re
import time
import textwrap
from dataclasses import dataclass, field
from typing import Any

import boto3
import streamlit as st
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, SystemMessagePromptTemplate, HumanMessagePromptTemplate
from langchain.schema import HumanMessage, SystemMessage
from langchain.schema.output_parser import StrOutputParser

# ─────────────────────────────────────────────
# Page config (must be first Streamlit call)
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="NYC TLC · Data Assistant",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# Custom CSS — dark industrial / data-terminal aesthetic
# ─────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    /* Global */
    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
        background-color: #0d0f14;
        color: #c9d1d9;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #10131a;
        border-right: 1px solid #1e2433;
    }
    section[data-testid="stSidebar"] h1, 
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] label {
        color: #e6edf3 !important;
        font-family: 'IBM Plex Mono', monospace;
    }

    /* Main header */
    .tlc-header {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.05rem;
        font-weight: 600;
        color: #f0c040;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        margin-bottom: 0.15rem;
    }
    .tlc-sub {
        font-size: 0.78rem;
        color: #586069;
        font-family: 'IBM Plex Mono', monospace;
        margin-bottom: 1.2rem;
    }

    /* Chat bubbles */
    .bubble-user {
        background: #1c2333;
        border: 1px solid #2d3748;
        border-radius: 12px 12px 2px 12px;
        padding: 0.75rem 1rem;
        margin: 0.4rem 0 0.4rem 3rem;
        color: #e6edf3;
        font-size: 0.9rem;
        line-height: 1.55;
    }
    .bubble-assistant {
        background: #161b27;
        border: 1px solid #1e2d40;
        border-left: 3px solid #f0c040;
        border-radius: 2px 12px 12px 12px;
        padding: 0.75rem 1rem;
        margin: 0.4rem 3rem 0.4rem 0;
        color: #c9d1d9;
        font-size: 0.9rem;
        line-height: 1.6;
    }
    .bubble-label {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.65rem;
        font-weight: 600;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #586069;
        margin-bottom: 0.3rem;
    }
    .bubble-label-user { color: #58a6ff; }
    .bubble-label-assistant { color: #f0c040; }

    /* SQL expander */
    .sql-block {
        background: #0d1117;
        border: 1px solid #21262d;
        border-radius: 6px;
        padding: 0.6rem 0.8rem;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.76rem;
        color: #79c0ff;
        margin-top: 0.5rem;
        white-space: pre-wrap;
        word-break: break-all;
    }

    /* KPI cards */
    .kpi-card {
        background: #10131a;
        border: 1px solid #1e2433;
        border-radius: 10px;
        padding: 1rem 1.1rem;
        margin-bottom: 0.75rem;
        position: relative;
        overflow: hidden;
    }
    .kpi-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0;
        width: 3px; height: 100%;
        background: #f0c040;
    }
    .kpi-rank {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.6rem;
        color: #586069;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        margin-bottom: 0.2rem;
    }
    .kpi-label {
        font-size: 0.8rem;
        color: #8b949e;
        margin-bottom: 0.35rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .kpi-value {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.45rem;
        font-weight: 600;
        color: #e6edf3;
        line-height: 1;
    }
    .kpi-delta {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem;
        color: #3fb950;
        margin-top: 0.2rem;
    }
    .kpi-panel-title {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.68rem;
        color: #586069;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        margin-bottom: 0.8rem;
        padding-bottom: 0.4rem;
        border-bottom: 1px solid #1e2433;
    }
    .kpi-empty {
        color: #30363d;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.78rem;
        text-align: center;
        padding: 2rem 0;
    }

    /* Chat input styling */
    .stTextInput > div > div > input {
        background-color: #161b27 !important;
        border: 1px solid #2d3748 !important;
        color: #e6edf3 !important;
        font-family: 'IBM Plex Sans', sans-serif !important;
        border-radius: 8px !important;
    }

    /* Buttons */
    .stButton > button {
        background-color: #f0c040 !important;
        color: #0d0f14 !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-weight: 600 !important;
        border: none !important;
        border-radius: 6px !important;
        font-size: 0.8rem !important;
        letter-spacing: 0.05em !important;
    }
    .stButton > button:hover {
        background-color: #d4a800 !important;
    }

    /* Dividers */
    hr { border-color: #1e2433; }

    /* Scrollable chat area */
    .chat-scroll {
        max-height: 62vh;
        overflow-y: auto;
        padding-right: 0.5rem;
    }

    /* Spinner */
    .stSpinner > div { border-top-color: #f0c040 !important; }

    /* Hide default Streamlit footer */
    footer { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────
# Config / env
# ─────────────────────────────────────────────
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "nyc_tlc_gold")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "nyc-tlc-dev")
ATHENA_RESULTS_BUCKET = os.getenv("ATHENA_RESULTS_BUCKET", "s3://nyc-tlc-athena-results-dev/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

GOLD_TABLES = ["fact_trips", "dim_location", "dim_datetime", "mart_hourly_kpi"]

# ─────────────────────────────────────────────
# Data types
# ─────────────────────────────────────────────

@dataclass
class ChatMessage:
    role: str          # "user" | "assistant"
    content: str
    sql: str = ""
    rows: list[dict[str, Any]] = field(default_factory=list)
    latency_ms: float = 0.0


@dataclass
class KpiCard:
    rank: int
    label: str
    value: str
    raw_value: float | None = None


# ─────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────

def _init_state() -> None:
    if "messages" not in st.session_state:
        st.session_state.messages: list[ChatMessage] = []
    if "kpi_cards" not in st.session_state:
        st.session_state.kpi_cards: list[KpiCard] = []
    if "total_queries" not in st.session_state:
        st.session_state.total_queries = 0


_init_state()

# ─────────────────────────────────────────────
# Athena executor
# ─────────────────────────────────────────────

@st.cache_resource
def _athena_client():
    return boto3.client("athena", region_name=AWS_REGION)


def run_athena(sql: str, max_rows: int = 50) -> list[dict[str, Any]]:
    client = _athena_client()
    resp = client.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_BUCKET},
    )
    exec_id = resp["QueryExecutionId"]

    # Poll
    while True:
        status = client.get_query_execution(QueryExecutionId=exec_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena {state}: {reason}")
        time.sleep(1.5)

    # Fetch rows
    paginator = client.get_paginator("get_query_results")
    rows: list[dict] = []
    headers: list[str] = []
    for page in paginator.paginate(QueryExecutionId=exec_id, PaginationConfig={"MaxItems": max_rows + 1}):
        result_rows = page["ResultSet"]["Rows"]
        if not headers:
            headers = [c["VarCharValue"] for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append({headers[i]: cell.get("VarCharValue", "") for i, cell in enumerate(row["Data"])})
    return rows[:max_rows]


# ─────────────────────────────────────────────
# Schema fetcher (Glue → DDL string for prompt)
# ─────────────────────────────────────────────

@st.cache_data(ttl=600)
def fetch_schema_ddl() -> str:
    try:
        glue = boto3.client("glue", region_name=AWS_REGION)
        parts: list[str] = []
        for table in GOLD_TABLES:
            try:
                resp = glue.get_table(DatabaseName=ATHENA_DATABASE, Name=table)
                sd = resp["Table"]["StorageDescriptor"]
                cols = sd.get("Columns", [])
                partitions = resp["Table"].get("PartitionKeys", [])
                col_lines = ", ".join(
                    f"{c['Name']} {c['Type']}" for c in cols
                )
                part_clause = ""
                if partitions:
                    part_clause = f" PARTITIONED BY ({', '.join(p['Name'] for p in partitions)})"
                parts.append(f"-- {ATHENA_DATABASE}.{table}\nCREATE TABLE {ATHENA_DATABASE}.{table} ({col_lines}){part_clause};")
            except Exception:
                parts.append(f"-- {ATHENA_DATABASE}.{table}  (schema unavailable)")
        return "\n\n".join(parts)
    except Exception:
        # Fallback minimal schema so the app still works offline / in dev
        return textwrap.dedent(f"""
            -- {ATHENA_DATABASE}.fact_trips
            CREATE TABLE {ATHENA_DATABASE}.fact_trips (
                trip_id STRING, cab_type STRING, pickup_date DATE,
                pickup_hour INT, pickup_location_id INT, dropoff_location_id INT,
                payment_type STRING, fare_amount DOUBLE, tip_amount DOUBLE,
                trip_distance DOUBLE, duration_minutes DOUBLE
            ) PARTITIONED BY (pickup_date);

            -- {ATHENA_DATABASE}.mart_hourly_kpi
            CREATE TABLE {ATHENA_DATABASE}.mart_hourly_kpi (
                pickup_date DATE, pickup_hour INT, cab_type STRING,
                pickup_zone STRING, dropoff_zone STRING, borough STRING,
                payment_type STRING, trip_count BIGINT, total_revenue DOUBLE,
                avg_fare_amount DOUBLE, avg_trip_distance DOUBLE, avg_duration_min DOUBLE
            ) PARTITIONED BY (pickup_date);

            -- {ATHENA_DATABASE}.dim_location
            CREATE TABLE {ATHENA_DATABASE}.dim_location (
                location_id INT, zone STRING, borough STRING, service_zone STRING
            );
        """).strip()


# ─────────────────────────────────────────────
# LangChain / OpenAI RAG chain
# ─────────────────────────────────────────────

SQL_SYSTEM = textwrap.dedent("""\
    You are an expert data analyst. You write correct Presto/Athena SQL.

    Available tables:
    {schema_ddl}

    Rules:
    - Always qualify tables as database.table (e.g. {database}.fact_trips).
    - Filter on partition columns (pickup_date) whenever possible.
    - Never use SELECT *; list columns explicitly.
    - Add LIMIT {{max_rows}} to every query.
    - Return ONLY the raw SQL — no explanation, no markdown fences.
""")

ANSWER_SYSTEM = textwrap.dedent("""\
    You are a data analyst assistant. Given a user question and Athena query results
    (as a markdown table), write a concise, accurate plain-English answer.
    - Ground every claim in the data; do not hallucinate numbers.
    - If no rows returned, explain that no data matched and suggest why.
    - Keep the answer under 130 words.
""")


def _build_llm(model: str, temperature: float) -> ChatOpenAI:
    return ChatOpenAI(
        model=model,
        temperature=temperature,
        openai_api_key=st.session_state.get("openai_api_key") or os.getenv("OPENAI_API_KEY", ""),
        streaming=False,
    )


def _rows_to_markdown(rows: list[dict]) -> str:
    if not rows:
        return "_No rows returned._"
    headers = list(rows[0].keys())
    header_line = " | ".join(headers)
    sep = " | ".join("---" for _ in headers)
    body = "\n".join("| " + " | ".join(str(r.get(h, "")) for h in headers) + " |" for r in rows)
    return f"| {header_line} |\n| {sep} |\n{body}"


def run_rag(
    question: str,
    model: str,
    temperature: float,
    max_rows: int,
    max_retries: int = 2,
) -> tuple[str, str, list[dict]]:
    """
    Returns (answer, sql, rows).
    Two-stage: SQL generation (with self-correct retry) → answer synthesis.
    """
    llm = _build_llm(model, temperature)
    schema_ddl = fetch_schema_ddl()

    # ── Stage 1: generate + execute SQL ──────────────────────────────
    sql_system = SQL_SYSTEM.format(
        schema_ddl=schema_ddl,
        database=ATHENA_DATABASE,
    ).replace("{max_rows}", str(max_rows))

    messages: list = [
        SystemMessage(content=sql_system),
        HumanMessage(content=question),
    ]

    last_error = ""
    sql = ""
    rows: list[dict] = []

    for attempt in range(max_retries + 1):
        if last_error and attempt > 0:
            messages.append(HumanMessage(
                content=f"Your previous SQL failed with:\n{last_error}\nRewrite it to fix the error. Return ONLY raw SQL."
            ))

        response = llm.invoke(messages)
        sql = response.content.strip().rstrip(";")

        try:
            rows = run_athena(sql, max_rows=max_rows)
            break  # success
        except RuntimeError as exc:
            last_error = str(exc)
            messages.append(response)  # keep assistant turn in history
    else:
        # All retries exhausted — return empty rows with last SQL
        rows = []

    # ── Stage 2: synthesise answer ────────────────────────────────────
    md_table = _rows_to_markdown(rows)
    answer_prompt = [
        SystemMessage(content=ANSWER_SYSTEM),
        HumanMessage(content=f"Question: {question}\n\nQuery results:\n{md_table}"),
    ]
    answer_response = llm.invoke(answer_prompt)
    answer = answer_response.content.strip()

    return answer, sql, rows


# ─────────────────────────────────────────────
# KPI card builder — extract top-5 from rows
# ─────────────────────────────────────────────

def _format_value(raw: str) -> str:
    """Best-effort numeric formatting."""
    try:
        num = float(raw)
        if num >= 1_000_000:
            return f"{num/1_000_000:.2f}M"
        if num >= 1_000:
            return f"{num:,.0f}"
        if num != int(num):
            return f"{num:,.2f}"
        return f"{int(num):,}"
    except (ValueError, TypeError):
        return str(raw)[:22]


def build_kpi_cards(rows: list[dict]) -> list[KpiCard]:
    """
    Turn the top-5 result rows into KPI cards.
    Heuristic: first column = label, last numeric column = value.
    """
    if not rows:
        return []

    headers = list(rows[0].keys())
    label_col = headers[0]

    # Find rightmost numeric column (excluding the label column)
    value_col = label_col
    for h in reversed(headers):
        if h == label_col:
            continue
        try:
            float(rows[0][h])
            value_col = h
            break
        except (ValueError, TypeError):
            pass

    cards: list[KpiCard] = []
    for i, row in enumerate(rows[:5], start=1):
        raw = row.get(value_col, "")
        cards.append(KpiCard(
            rank=i,
            label=str(row.get(label_col, f"Item {i}")),
            value=_format_value(raw),
            raw_value=float(raw) if _is_numeric(raw) else None,
        ))
    return cards


def _is_numeric(v: str) -> bool:
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


# ─────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────

with st.sidebar:
    st.markdown('<div class="tlc-header">🚕 NYC TLC</div>', unsafe_allow_html=True)
    st.markdown('<div class="tlc-sub">Data Assistant · Gold Layer</div>', unsafe_allow_html=True)
    st.markdown("---")

    st.markdown("#### ⚙️ Settings")

    openai_key = st.text_input(
        "OpenAI API Key",
        type="password",
        value=os.getenv("OPENAI_API_KEY", ""),
        help="Your sk-... key. Stored only in session memory.",
    )
    st.session_state["openai_api_key"] = openai_key

    model = st.selectbox(
        "Model",
        ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"],
        index=0,
    )

    temperature = st.slider("Temperature", 0.0, 1.0, 0.0, 0.05,
                            help="0 = deterministic SQL; higher = more creative answers")

    max_rows = st.slider("Max result rows", 5, 200, 50, 5)

    max_retries = st.selectbox("SQL self-correct retries", [0, 1, 2, 3], index=2)

    st.markdown("---")
    st.markdown("#### 💡 Example questions")
    examples = [
        "Top 5 pickup zones by trip count in Jan 2023?",
        "Average fare by payment type last quarter?",
        "Which borough had most trips per hour on weekdays?",
        "Compare yellow vs green cab revenue in Q1 2023.",
        "Top 5 drop-off zones from JFK airport?",
    ]
    for ex in examples:
        if st.button(ex, key=f"ex_{ex[:20]}"):
            st.session_state["prefill"] = ex

    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Queries", st.session_state.total_queries)
    with col2:
        st.metric("Messages", len(st.session_state.messages))

    if st.button("🗑 Clear chat"):
        st.session_state.messages = []
        st.session_state.kpi_cards = []
        st.session_state.total_queries = 0
        st.rerun()


# ─────────────────────────────────────────────
# Main layout — chat (left 65%) + KPI panel (right 35%)
# ─────────────────────────────────────────────

chat_col, kpi_col = st.columns([65, 35], gap="large")

# ── KPI panel ──────────────────────────────────────────────────────────
with kpi_col:
    st.markdown('<div class="kpi-panel-title">▸ Top 5 Results</div>', unsafe_allow_html=True)

    if st.session_state.kpi_cards:
        for card in st.session_state.kpi_cards:
            # Accent colour cycles through yellow → teal → blue
            accent_colors = ["#f0c040", "#3fb950", "#58a6ff", "#d2a8ff", "#ffa657"]
            accent = accent_colors[(card.rank - 1) % len(accent_colors)]
            st.markdown(
                f"""
                <div class="kpi-card" style="border-left-color: {accent}">
                    <div class="kpi-rank">#{card.rank}</div>
                    <div class="kpi-label" title="{card.label}">{card.label}</div>
                    <div class="kpi-value">{card.value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            '<div class="kpi-empty">Ask a question to<br/>see top results here.</div>',
            unsafe_allow_html=True,
        )

# ── Chat panel ─────────────────────────────────────────────────────────
with chat_col:
    st.markdown('<div class="tlc-header" style="font-size:0.9rem">NYC TLC · Data Assistant</div>', unsafe_allow_html=True)
    st.markdown('<div class="tlc-sub">Ask anything about NYC taxi trips — I\'ll query the Gold layer for you.</div>', unsafe_allow_html=True)

    # Render message history
    chat_container = st.container()
    with chat_container:
        for msg in st.session_state.messages:
            if msg.role == "user":
                st.markdown(
                    f'<div class="bubble-label bubble-label-user">You</div>'
                    f'<div class="bubble-user">{msg.content}</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="bubble-label bubble-label-assistant">🚕 Assistant</div>'
                    f'<div class="bubble-assistant">{msg.content}</div>',
                    unsafe_allow_html=True,
                )
                if msg.sql:
                    with st.expander("View generated SQL", expanded=False):
                        st.markdown(
                            f'<div class="sql-block">{msg.sql}</div>',
                            unsafe_allow_html=True,
                        )
                if msg.rows:
                    with st.expander(f"Raw data ({len(msg.rows)} rows)", expanded=False):
                        st.dataframe(msg.rows, use_container_width=True, height=180)

    st.markdown("---")

    # Input row
    prefill = st.session_state.pop("prefill", "")
    user_input = st.text_input(
        "Ask a question about NYC TLC data…",
        value=prefill,
        key="chat_input",
        label_visibility="collapsed",
        placeholder="e.g. Top 5 pickup zones by trip count in January 2023?",
    )

    send_col, _ = st.columns([1, 5])
    with send_col:
        send = st.button("Send →", use_container_width=True)

    # ── Process query ───────────────────────────────────────────────────
    if send and user_input.strip():
        question = user_input.strip()

        if not st.session_state.get("openai_api_key"):
            st.error("Please enter your OpenAI API key in the sidebar.")
            st.stop()

        # Append user message
        st.session_state.messages.append(ChatMessage(role="user", content=question))
        st.session_state.total_queries += 1

        with st.spinner("Querying Athena…"):
            t0 = time.perf_counter()
            try:
                answer, sql, rows = run_rag(
                    question=question,
                    model=model,
                    temperature=temperature,
                    max_rows=max_rows,
                    max_retries=max_retries,
                )
                latency_ms = (time.perf_counter() - t0) * 1000

                # Append assistant message
                st.session_state.messages.append(
                    ChatMessage(
                        role="assistant",
                        content=answer,
                        sql=sql,
                        rows=rows,
                        latency_ms=round(latency_ms, 1),
                    )
                )

                # Update KPI cards from new rows
                st.session_state.kpi_cards = build_kpi_cards(rows)

            except Exception as exc:
                st.session_state.messages.append(
                    ChatMessage(
                        role="assistant",
                        content=f"⚠️ Something went wrong: {exc}",
                    )
                )

        st.rerun()