# ============================================================
# Stage 1 — dependency builder
# ============================================================
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./

# Install all deps into an isolated prefix
RUN pip install --upgrade pip \
    && pip install --prefix=/install --no-cache-dir \
        fastapi==0.111.* \
        uvicorn[standard]==0.29.* \
        pydantic==2.* \
        pydantic-settings==2.* \
        anthropic==0.26.* \
        openai==1.30.* \
        langchain==0.2.* \
        langchain-openai==0.1.* \
        langchain-community==0.2.* \
        boto3==1.34.* \
        botocore==1.34.* \
        streamlit==1.35.* \
        httpx==0.27.* \
        mlflow==2.13.* \
        supervisor==4.2.*


# ============================================================
# Stage 2 — lean runtime image
# ============================================================
FROM python:3.11-slim AS runtime

RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --shell /bin/bash --create-home appuser

WORKDIR /app

COPY --from=builder /install /usr/local

COPY app.py rag_chain.py streamlit_app.py ./

RUN mkdir -p /etc/supervisor/conf.d /var/log/supervisor
COPY supervisord.conf /etc/supervisor/conf.d/nyc-tlc.conf

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ATHENA_DATABASE="nyc_tlc_gold" \
    ATHENA_WORKGROUP="nyc-tlc-dev" \
    ATHENA_RESULTS_BUCKET="s3://nyc-tlc-athena-results-dev/" \
    AWS_REGION="us-east-1" \
    OPENAI_API_KEY="" \
    ANTHROPIC_MODEL="claude-sonnet-4-20250514" \
    MAX_SQL_RETRIES="2" \
    LOG_LEVEL="INFO" \
    CORS_ORIGINS="*" \
    APP_HOST="0.0.0.0" \
    APP_PORT="8000" \
    APP_WORKERS="2" \
    STREAMLIT_PORT="8501" \
    STREAMLIT_SERVER_HEADLESS="true" \
    STREAMLIT_SERVER_ENABLE_CORS="false" \
    STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION="false" \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS="false"

USER appuser

EXPOSE 8000 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c \
        "import urllib.request, sys; \
         r = urllib.request.urlopen('http://localhost:8000/health', timeout=8); \
         sys.exit(0 if r.status == 200 else 1)"

CMD ["supervisord", "-c", "/etc/supervisor/conf.d/nyc-tlc.conf", "-n"]