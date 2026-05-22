"""
serving/rag_app/streamlit_app.py

Enhanced Streamlit UI for the NYC TLC RAG assistant.

New features vs original
-------------------------
1. Auto-visualisation  — detects numeric columns in the result set and renders
                         a bar or line chart automatically (st.bar_chart /
                         st.line_chart).  A radio button lets the user switch
                         chart type or turn it off.
2. Data Insights panel — a second LangChain chain call produces 3 bullet-point
                         observations about the result set (trend, outlier,
                         comparison) shown in a collapsible expander.
3. Follow-up questions — RagAnswer now carries `follow_up_questions`; each is
                         rendered as a clickable st.button that re-submits the
                         question automatically.
4. Copy SQL button     — st.code with a copy icon; also a standalone
                         st.download_button to save the SQL as a .sql file.
5. Sidebar controls    — max_rows slider, model selector, temperature slider.
6. Result table filter — a text input above st.dataframe lets users filter rows
                         client-side without a new Athena query.

asyncio fix (unchanged from original)
--------------------------------------
nest_asyncio.apply() patches Streamlit's running event loop so asyncio.run()
works inside it. Requires: nest_asyncio>=1.6
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import nest_asyncio
import pandas as pd
import streamlit as st
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from rag_chain import NycTlcRagChain, RagAnswer

# Patch Streamlit's event loop once at startup.
nest_asyncio.apply()

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="NYC TLC RAG",
    page_icon="🚕",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar — configuration controls
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("⚙️ Settings")

    model_id = st.selectbox(
        "OpenAI model",
        ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"],
        index=0,
        help="Model used for both SQL generation and answer synthesis.",
    )
    temperature = st.slider(
        "Temperature",
        min_value=0.0, max_value=1.0, value=0.0, step=0.05,
        help="0 = deterministic SQL; raise for more creative answers.",
    )
    max_rows = st.slider(
        "Max rows returned",
        min_value=5, max_value=200, value=50, step=5,
        help="Athena LIMIT clause value.",
    )
    max_retries = st.slider(
        "Max SQL retries",
        min_value=0, max_value=5, value=2,
        help="How many times the chain re-tries a failing SQL.",
    )

    st.divider()
    st.caption("🔍 MLflow experiment: **nyc-tlc-rag**")
    if st.button("🗑️ Clear chat history"):
        st.session_state.messages = []
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state.messages = []

if "pending_question" not in st.session_state:
    st.session_state.pending_question = None

# ─────────────────────────────────────────────────────────────────────────────
# RAG chain — cached per (model_id, temperature, max_retries) combo
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_rag_chain(model: str, temp: float, retries: int) -> NycTlcRagChain:
    return NycTlcRagChain(
        athena_database=os.getenv("ATHENA_DATABASE", "nyc_tlc_gold"),
        athena_workgroup=os.getenv("ATHENA_WORKGROUP", "nyc-tlc-dev"),
        athena_results_location=os.getenv("ATHENA_RESULTS_BUCKET"),
        aws_region=os.getenv("AWS_REGION", "ap-southeast-1"),
        model_id=model,
        max_sql_retries=retries,
        temperature=temp,
    )


rag = get_rag_chain(model_id, temperature, max_retries)

# ─────────────────────────────────────────────────────────────────────────────
# Data Insights chain — lightweight, separate from the main RAG chain
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_insights_chain(model: str):
    _INSIGHTS_SYSTEM = (
        "You are a concise data analyst. Given a markdown table of query results, "
        "write exactly 3 short bullet-point observations (trend, outlier, or comparison). "
        "Be specific — mention actual values. Keep each bullet under 20 words. "
        "Return ONLY the 3 bullets, nothing else."
    )
    llm = ChatOpenAI(model=model, temperature=0.3)
    prompt = ChatPromptTemplate.from_messages([
        ("system", _INSIGHTS_SYSTEM),
        ("human", "Question asked: {question}\n\nData:\n{markdown_table}"),
    ])
    return prompt | llm | StrOutputParser()


insights_chain = get_insights_chain(model_id)


async def generate_insights(question: str, markdown_table: str) -> list[str]:
    raw = await insights_chain.ainvoke(
        {"question": question, "markdown_table": markdown_table}
    )
    bullets = [
        line.lstrip("-•* ").strip()
        for line in raw.splitlines()
        if line.strip()
    ]
    return bullets[:3]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers: visualisation & table filtering
# ─────────────────────────────────────────────────────────────────────────────

def rows_to_markdown(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "_No rows returned._"
    headers = list(rows[0].keys())
    sep = " | ".join("---" for _ in headers)
    return (
        f"| {' | '.join(headers)} |\n"
        f"| {sep} |\n"
        + "\n".join(
            f"| {' | '.join(str(r.get(h, '')) for h in headers)} |"
            for r in rows
        )
    )


def _coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Try to cast object columns to numeric where possible."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")
    return df


def render_chart(df: pd.DataFrame, chart_type: str) -> None:
    """
    Render bar or line chart when there's at least one numeric column.
    Picks the first string column as the x-axis index.
    """
    df = _coerce_numeric(df.copy())
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    string_cols  = df.select_dtypes(exclude="number").columns.tolist()

    if not numeric_cols:
        st.info("No numeric columns detected — chart unavailable.")
        return

    if string_cols:
        df = df.set_index(string_cols[0])

    chart_df = df[numeric_cols]

    if chart_type == "Bar":
        st.bar_chart(chart_df)
    elif chart_type == "Line":
        st.line_chart(chart_df)
    elif chart_type == "Area":
        st.area_chart(chart_df)


# ─────────────────────────────────────────────────────────────────────────────
# Render a single assistant message (used for history replay + new answers)
# ─────────────────────────────────────────────────────────────────────────────

def render_assistant_message(msg: dict) -> None:
    st.markdown(msg["content"])

    # ── SQL expander with download button ─────────────────────────────────
    if msg.get("sql"):
        with st.expander("🗃️ Generated SQL", expanded=False):
            st.code(msg["sql"], language="sql")
            st.download_button(
                label="⬇️ Download .sql",
                data=msg["sql"],
                file_name="query.sql",
                mime="text/plain",
                key=f"dl_sql_{msg.get('_id', id(msg))}",
            )

    # ── Raw data table with filter ─────────────────────────────────────────
    raw_rows = msg.get("raw_rows")
    if raw_rows:
        df = pd.DataFrame(raw_rows)

        filter_text = st.text_input(
            "🔎 Filter rows (searches all columns)",
            key=f"filter_{msg.get('_id', id(msg))}",
            placeholder="Type to filter…",
        )
        if filter_text:
            mask = df.apply(
                lambda col: col.astype(str).str.contains(filter_text, case=False, na=False)
            ).any(axis=1)
            df = df[mask]

        st.dataframe(df, use_container_width=True)

        # ── Chart ──────────────────────────────────────────────────────────
        chart_type = st.radio(
            "Chart type",
            ["Bar", "Line", "Area", "None"],
            horizontal=True,
            index=0,
            key=f"chart_{msg.get('_id', id(msg))}",
        )
        if chart_type != "None":
            render_chart(pd.DataFrame(raw_rows), chart_type)

    # ── Data Insights ──────────────────────────────────────────────────────
    if msg.get("insights"):
        with st.expander("💡 Data Insights", expanded=True):
            for bullet in msg["insights"]:
                st.markdown(f"- {bullet}")

    # ── Run metadata ───────────────────────────────────────────────────────
    if msg.get("sql_attempts") is not None:
        cols = st.columns(3)
        cols[0].metric("SQL Attempts",  msg["sql_attempts"])
        cols[1].metric("Rows Returned", msg.get("rows_returned", "—"))
        cols[2].metric("Latency (s)",   msg.get("latency_seconds", "—"))

    # ── Follow-up question buttons ─────────────────────────────────────────
    follow_ups = msg.get("follow_up_questions", [])
    if follow_ups:
        st.markdown("**Suggested follow-ups:**")
        cols = st.columns(len(follow_ups))
        for i, fq in enumerate(follow_ups):
            if cols[i].button(f"💬 {fq}", key=f"fq_{msg.get('_id', id(msg))}_{i}"):
                st.session_state.pending_question = fq
                st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Page title & description
# ─────────────────────────────────────────────────────────────────────────────

st.title("🚕 NYC TLC RAG Assistant")
st.caption(
    "Ask natural-language questions about NYC taxi data. "
    "Answers are grounded in live Athena query results."
)

# ─────────────────────────────────────────────────────────────────────────────
# Chat history (replay)
# ─────────────────────────────────────────────────────────────────────────────

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        if msg["role"] == "assistant":
            render_assistant_message(msg)
        else:
            st.markdown(msg["content"])

# ─────────────────────────────────────────────────────────────────────────────
# Input: typed question OR a pending follow-up from a button click
# ─────────────────────────────────────────────────────────────────────────────

typed_question = st.chat_input("Ask about NYC TLC data…")
question = st.session_state.pending_question or typed_question

if st.session_state.pending_question:
    st.session_state.pending_question = None

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Querying Athena and generating answer…"):
            try:
                result: RagAnswer = asyncio.run(rag.ainvoke(question, max_rows=max_rows))

                # Generate insights alongside the answer
                markdown_table = rows_to_markdown(result.raw_rows) if result.raw_rows else ""
                insights: list[str] = []
                if result.raw_rows:
                    try:
                        insights = asyncio.run(
                            generate_insights(question, markdown_table)
                        )
                    except Exception:
                        insights = []

                msg_record: dict = {
                    "role":               "assistant",
                    "content":            result.answer,
                    "sql":                result.sql,
                    "raw_rows":           result.raw_rows,
                    "sql_attempts":       result.sql_attempts,
                    "rows_returned":      result.rows_returned,
                    "latency_seconds":    result.latency_seconds,
                    "follow_up_questions": result.follow_up_questions,
                    "insights":           insights,
                    "_id":                len(st.session_state.messages),
                }
                st.session_state.messages.append(msg_record)
                render_assistant_message(msg_record)

            except Exception as exc:
                error_msg = f"❌ Error: {exc}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role":    "assistant",
                    "content": error_msg,
                })