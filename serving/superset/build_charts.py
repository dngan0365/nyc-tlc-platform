"""
serving/superset/build_charts.py
─────────────────────────────────
Auto-build all NYC TLC charts + dashboard in Superset via REST API.

FIXES vs original
-----------------
1. GLUE_DATABASE_MARTS = "nyc_tlc_gold_marts"  ← all mart tables live here
2. All chart column names corrected to match actual Glue schema:
     fact_trips     → trip_id, cab_type, payment_type, pickup_at,
                       pickup_date, pickup_hour, trip_distance_miles,
                       fare_amount, total_amount, tip_amount,
                       pickup_location_id, passenger_count
     mart_hourly_kpi→ pickup_date, pickup_hour, cab_type, borough_group,
                       trip_count, total_fare, avg_fare, avg_distance_miles,
                       avg_duration_minutes, avg_speed_mph, year_month,
                       day_name, is_airport, is_weekend
     dim_location   → location_id, zone_name, borough, borough_group,
                       service_zone, is_airport, is_manhattan_cbd
3. Timeseries charts now include granularity_sqla + time_grain_sqla
4. Dashboard grid uses width=6 per chart (two per row, 6+6=12)
5. Schema passed to get_or_create_dataset uses GLUE_DATABASE_MARTS
6. RAG chain constants (GOLD_TABLES, _SQL_SYSTEM) updated to match

Environment variables (all come from .env / .env.generated):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION,
    ATHENA_RESULTS_BUCKET, DBT_ATHENA_WORKGROUP,
    GLUE_DATABASE_GOLD        (raw/gold Delta tables  → nyc_tlc_gold)
    GLUE_DATABASE_MARTS       (dbt mart tables        → nyc_tlc_gold_marts)
    SUPERSET_URL, SUPERSET_USER, SUPERSET_PASS,
    SUPERSET_WAIT_TIMEOUT
"""

from __future__ import annotations

import os
import json
import time
import logging
from typing import Any

import boto3
import requests

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Config — 100 % from environment
# ──────────────────────────────────────────────────────────────────────────────

SUPERSET_URL  = os.environ.get("SUPERSET_URL",  "http://superset:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASS = os.environ.get("SUPERSET_PASS", "admin")
WAIT_TIMEOUT  = int(os.environ.get("SUPERSET_WAIT_TIMEOUT", "120"))
WAIT_INTERVAL = 5

AWS_REGION            = os.environ.get("AWS_DEFAULT_REGION",    "ap-southeast-1")
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID",     "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
ATHENA_RESULTS_BUCKET = os.environ.get("ATHENA_RESULTS_BUCKET", "")
DBT_ATHENA_WORKGROUP  = os.environ.get("DBT_ATHENA_WORKGROUP",  "primary")

# FIX 1 ── marts tables are in nyc_tlc_gold_marts, NOT nyc_tlc_gold
GLUE_DATABASE_GOLD  = os.environ.get("GLUE_DATABASE_GOLD",   "nyc_tlc_gold")
GLUE_DATABASE_MARTS = os.environ.get("GLUE_DATABASE_MARTS",  "nyc_tlc_gold_marts")

SUPERSET_DB_NAME   = f"Athena {GLUE_DATABASE_MARTS}"
ATHENA_S3_STAGING  = f"s3://{ATHENA_RESULTS_BUCKET}/superset/"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _athena_sqlalchemy_uri() -> str:
    return (
        f"awsathena+rest://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}"
        f"@athena.{AWS_REGION}.amazonaws.com:443/{GLUE_DATABASE_MARTS}"
        f"?s3_staging_dir={ATHENA_S3_STAGING}"
        f"&work_group={DBT_ATHENA_WORKGROUP}"
        f"&region_name={AWS_REGION}"
    )


def verify_athena_connection() -> None:
    try:
        client = boto3.client(
            "athena",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
        )
        client.get_work_group(WorkGroup=DBT_ATHENA_WORKGROUP)
        log.info("Athena credentials OK (workgroup=%s) ✓", DBT_ATHENA_WORKGROUP)
    except Exception as exc:
        raise RuntimeError(
            f"Athena connectivity check failed: {exc}\n"
            "Make sure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, "
            "AWS_DEFAULT_REGION and DBT_ATHENA_WORKGROUP are set correctly."
        ) from exc


# ──────────────────────────────────────────────────────────────────────────────
# Wait for Superset
# ──────────────────────────────────────────────────────────────────────────────

def wait_for_superset() -> None:
    deadline = time.time() + WAIT_TIMEOUT
    url = f"{SUPERSET_URL}/health"
    log.info("Waiting for Superset at %s …", url)
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                log.info("Superset is ready ✓")
                return
        except requests.exceptions.ConnectionError:
            pass
        log.info("  not ready — retrying in %ss", WAIT_INTERVAL)
        time.sleep(WAIT_INTERVAL)
    raise RuntimeError(f"Superset did not become ready within {WAIT_TIMEOUT}s.")

# ──────────────────────────────────────────────────────────────────────────────
# Dashboard grid layout
# ──────────────────────────────────────────────────────────────────────────────
def _build_grid_layout(chart_ids: list[int]) -> dict:
    # Khởi tạo khung layout v2 cơ bản của Superset
    layout: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID":   {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID":   {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": "NYC TLC Analytics"}},
    }
    row_ids: list[str] = []

    for i, chart_id in enumerate(chart_ids):
        row_idx   = i // 2
        col_idx   = i %  2
        row_id    = f"ROW-layout-{row_idx}"
        # SỬA: Key định danh của component ô chứa chart nên viết hoa chuẩn cấu trúc layout của Superset
        chart_key = f"CHART-{chart_id}" 

        if col_idx == 0:
            layout[row_id] = {
                "type": "ROW", 
                "id": row_id, 
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID"],  # Định nghĩa chính xác cây thư mục cha
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            row_ids.append(row_id)

        layout[row_id]["children"].append(chart_key)
        layout[chart_key] = {
            "type": "CHART", 
            "id": chart_key, 
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {
                "chartId": chart_id,  # Đây là ID số nguyên (int) thực tế từ API trả về
                "width":   6,
                "height":  50,
                "sliceName": f"Chart {chart_id}",
            },
        }

    layout["GRID_ID"]["children"] = row_ids
    return layout
# ──────────────────────────────────────────────────────────────────────────────
# Superset API client
# ──────────────────────────────────────────────────────────────────────────────

class SupersetClient:
    def __init__(self):
        self.base = SUPERSET_URL.rstrip("/")
        self.session = requests.Session()
        self._login()

    def _login(self) -> None:
        resp = self.session.post(
            f"{self.base}/api/v1/security/login",
            json={"username": SUPERSET_USER, "password": SUPERSET_PASS, "provider": "db"},
        )
        resp.raise_for_status()
        self.session.headers.update({
            "Authorization": f"Bearer {resp.json()['access_token']}",
            "Content-Type":  "application/json",
        })
        csrf = self.session.get(f"{self.base}/api/v1/security/csrf_token/")
        csrf.raise_for_status()
        self.session.headers["X-CSRFToken"] = csrf.json()["result"]
        log.info("Logged in to Superset ✓")

    # ── low-level ─────────────────────────────────────────────────────────────

    def get(self, path: str, **kwargs) -> Any:
        r = self.session.get(f"{self.base}{path}", **kwargs)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: dict) -> Any:
        r = self.session.post(f"{self.base}{path}", json=payload)
        if not r.ok:
            log.error("POST %s → %s  %s", path, r.status_code, r.text[:800])
        r.raise_for_status()
        return r.json()

    def put(self, path: str, payload: dict) -> Any:
        r = self.session.put(f"{self.base}{path}", json=payload)
        r.raise_for_status()
        return r.json()

    # ── database ──────────────────────────────────────────────────────────────

    def get_or_create_database(self) -> int:
        """
        Register the Athena connection pointing at nyc_tlc_gold_marts.
        Idempotent — safe to re-run.
        """
        data = self.get("/api/v1/database/", params={"q": json.dumps({"page_size": 100})})
        for db in data["result"]:
            if db["database_name"] == SUPERSET_DB_NAME:
                log.info(
                    "Database '%s' already registered → id=%s",
                    SUPERSET_DB_NAME, db["id"],
                )
                return db["id"]

        log.info("Registering Athena database '%s' …", SUPERSET_DB_NAME)
        result = self.post("/api/v1/database/", {
            "database_name":    SUPERSET_DB_NAME,
            "sqlalchemy_uri":   _athena_sqlalchemy_uri(),
            "expose_in_sqllab": True,
            "allow_run_async":  False,   # ← sync only, no Celery
            "allow_ctas":       False,
            "allow_cvas":       False,
            "extra": json.dumps({
                "engine_params": {
                    "connect_args": {"work_group": DBT_ATHENA_WORKGROUP}
                }
            }),
        })
        db_id = result["id"]
        log.info("Database registered → id=%s ✓", db_id)
        return db_id

    # ── dataset ───────────────────────────────────────────────────────────────

    def get_or_create_dataset(self, db_id: int, table: str) -> int:
        """
        Paginate through all datasets and match by table_name + schema.
        Avoids the broken filter API on Superset 3.1.x (returns 400).
        """
        page = 0
        page_size = 100
        while True:
            q = json.dumps({
                "page":      page,
                "page_size": page_size,
            })
            data = self.get("/api/v1/dataset/", params={"q": q})
            results = data.get("result", [])

            for ds in results:
                if (
                    ds.get("table_name") == table
                    and ds.get("schema") == GLUE_DATABASE_MARTS
                ):
                    log.info(
                        "Dataset %s.%s exists → id=%s",
                        GLUE_DATABASE_MARTS, table, ds["id"],
                    )
                    return ds["id"]

            if len(results) < page_size:
                break
            page += 1

        # Not found — create it
        result = self.post("/api/v1/dataset/", {
            "database":   db_id,
            "schema":     GLUE_DATABASE_MARTS,
            "table_name": table,
            # "catalog":    "AwsDataCatalog",
        })
        ds_id = result["id"]
        log.info("Created dataset %s.%s → id=%s", GLUE_DATABASE_MARTS, table, ds_id)
        return ds_id

    # ── chart / dashboard ─────────────────────────────────────────────────────

    def create_chart(self, payload: dict) -> int:
        result = self.post("/api/v1/chart/", payload)
        chart_id = result["id"]
        log.info("  ✓ chart '%s' → id=%s", payload["slice_name"], chart_id)
        return chart_id

    def create_dashboard(self, title: str, chart_ids: list[int]) -> int:
        # Gộp toàn bộ metadata cấu hình và layout vào một payload duy nhất để POST một lần
        payload = {
            "dashboard_title": title,
            "published":       True,
            "position_json":   json.dumps(_build_grid_layout(chart_ids)),
            "json_metadata": json.dumps({
                "chart_configuration": {
                    str(cid): {"id": cid, "crossFiltersEnabled": False}
                    for cid in chart_ids
                },
                "global_chart_configuration": {
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "chartsInScope": chart_ids,
                },
            }),
        }

        log.info("Creating dashboard '%s' with %d charts via single POST...", title, len(chart_ids))
        result = self.post("/api/v1/dashboard/", payload)
        dash_id = result["id"]
        
        log.info("Dashboard '%s' created successfully → id=%s ✓", title, dash_id)
        return dash_id



# ──────────────────────────────────────────────────────────────────────────────
# Chart definitions — corrected against actual Glue schema
# ──────────────────────────────────────────────────────────────────────────────
#
# fact_trips columns used below:
#   trip_id, cab_type, payment_type, pickup_at (timestamp), pickup_date,
#   pickup_hour, trip_distance_miles, fare_amount, total_amount,
#   tip_amount, passenger_count, pickup_location_id
#
# mart_hourly_kpi columns used below:
#   pickup_date, pickup_hour, cab_type, borough_group, trip_count,
#   total_fare, avg_fare, avg_distance_miles, avg_duration_minutes,
#   avg_speed_mph, year_month, day_name, is_airport, is_weekend
#
# dim_location columns used below:
#   location_id, zone_name, borough, borough_group, service_zone,
#   is_airport, is_manhattan_cbd
# ──────────────────────────────────────────────────────────────────────────────

def chart_specs(dataset_ids: dict[str, int]) -> list[dict]:
    fact = dataset_ids["fact_trips"]
    kpi  = dataset_ids["mart_hourly_kpi"]
    loc  = dataset_ids["dim_location"]

    def _metric(col: str, agg: str, label: str) -> dict:
        return {
            "expressionType": "SIMPLE",
            "column":         {"column_name": col},
            "aggregate":      agg,
            "label":          label,
        }

    return [
        # 1. Monthly trip volume (line) — uses year_month + trip_count
        {
            "slice_name":      "Monthly Trip Volume",
            "viz_type":        "echarts_timeseries_line",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":         "echarts_timeseries_line",
                "x_axis":           "year_month",
                "granularity_sqla": "pickup_date",
                "time_grain_sqla":  "P1M",
                "time_range":       "No filter",
                "metrics":          [_metric("trip_count", "SUM", "Trips")],
                "groupby":          [],
                "show_legend":      True,
                "rich_tooltip":     True,
            }),
        },

        # 2. Avg fare by borough group (bar)
        {
            "slice_name":      "Avg Fare by Borough Group",
            "viz_type":        "echarts_bar",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":    "echarts_bar",
                "metrics":     [_metric("avg_fare", "AVG", "Avg Fare (USD)")],
                "groupby":     ["borough_group"],
                "time_range":  "No filter",
                "show_legend": False,
            }),
        },

        # 3. Total trips big number
        {
            "slice_name":      "Total Trips – All Time",
            "viz_type":        "big_number_total",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":         "big_number_total",
                "metric":           _metric("trip_count", "SUM", "Trips"),
                "granularity_sqla": "pickup_date",
                "time_range":       "No filter",
                "subheader":        "total trips",
            }),
        },

        # 4. Total revenue big number
        {
            "slice_name":      "Total Revenue – All Time",
            "viz_type":        "big_number_total",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":         "big_number_total",
                "metric":           _metric("total_revenue", "SUM", "Revenue (USD)"),
                "granularity_sqla": "pickup_date",
                "time_range":       "No filter",
                "subheader":        "USD total revenue",
            }),
        },

        # 5. Trip volume heatmap hour x day
        {
            "slice_name":      "Trip Volume by Hour & Day",
            "viz_type":        "heatmap",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":            "heatmap",
                "all_columns_x":       "pickup_hour",
                "all_columns_y":       "day_name",
                "metric":              _metric("trip_count", "SUM", "Trips"),
                "time_range":          "No filter",
                "normalize_across":    "heatmap",
                "linear_color_scheme": "blue_white_yellow",
            }),
        },

        # 6. Borough group revenue table
        {
            "slice_name":      "Top Borough Groups by Revenue",
            "viz_type":        "table",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":      "table",
                "metrics":       [
                    _metric("trip_count",          "SUM", "Trips"),
                    _metric("total_revenue",        "SUM", "Total Revenue"),
                    _metric("avg_fare",             "AVG", "Avg Fare"),
                    _metric("avg_duration_minutes", "AVG", "Avg Duration (min)"),
                ],
                "groupby":       ["borough_group"],
                "time_range":    "No filter",
                "row_limit":     20,
                "order_desc":    True,
            }),
        },

        # 7. Avg trip duration over time (area)
        {
            "slice_name":      "Avg Trip Duration Over Time",
            "viz_type":        "echarts_area",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":         "echarts_area",
                "x_axis":           "pickup_date",
                "granularity_sqla": "pickup_date",
                "time_grain_sqla":  "P1D",
                "time_range":       "No filter",
                "metrics":          [_metric("avg_duration_minutes", "AVG", "Avg Duration (min)")],
                "groupby":          [],
                "show_legend":      False,
                "opacity":          0.4,
            }),
        },

        # 8. Payment type distribution (pie) — from fact_trips
        {
            "slice_name":      "Payment Type Distribution",
            "viz_type":        "pie",
            "datasource_id":   fact,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":    "pie",
                "metric":      _metric("trip_id", "COUNT", "Trips"),
                "groupby":     ["payment_type"],
                "time_range":  "No filter",
                "donut":       True,
                "show_legend": True,
                "label_type":  "key_percent",
            }),
        },

        # 9. Airport vs non-airport (bar)
        {
            "slice_name":      "Airport vs Non-Airport Trips",
            "viz_type":        "echarts_bar",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":    "echarts_bar",
                "metrics":     [_metric("trip_count", "SUM", "Trips")],
                "groupby":     ["is_airport"],
                "time_range":  "No filter",
                "show_legend": False,
            }),
        },

        # 10. Avg speed over time (line)
        {
            "slice_name":      "Avg Speed (mph) Over Time",
            "viz_type":        "echarts_timeseries_line",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":         "echarts_timeseries_line",
                "x_axis":           "pickup_date",
                "granularity_sqla": "pickup_date",
                "time_grain_sqla":  "P1D",
                "time_range":       "No filter",
                "metrics":          [_metric("avg_speed_mph", "AVG", "Avg Speed (mph)")],
                "groupby":          [],
                "show_legend":      False,
                "rich_tooltip":     True,
            }),
        },

        # 11. Weekend vs weekday trips (bar)
        {
            "slice_name":      "Weekend vs Weekday Trips",
            "viz_type":        "echarts_bar",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":    "echarts_bar",
                "metrics":     [_metric("trip_count", "SUM", "Trips")],
                "groupby":     ["is_weekend"],
                "time_range":  "No filter",
                "show_legend": False,
            }),
        },

        # 12. Revenue per mile by borough (bar)
        {
            "slice_name":      "Revenue per Mile by Borough",
            "viz_type":        "echarts_bar",
            "datasource_id":   kpi,
            "datasource_type": "table",
            "params": json.dumps({
                "viz_type":    "echarts_bar",
                "metrics":     [_metric("revenue_per_mile", "AVG", "Revenue/Mile (USD)")],
                "groupby":     ["borough_group"],
                "time_range":  "No filter",
                "show_legend": False,
                "order_desc":  True,
            }),
        },
    ]

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    # 0. Smoke-test AWS credentials
    verify_athena_connection()

    # 1. Wait for Superset
    wait_for_superset()

    client = SupersetClient()

    # 2. Auto-register Athena database → nyc_tlc_gold_marts
    db_id = client.get_or_create_database()

    # 3. Datasets — all from nyc_tlc_gold_marts
    dataset_ids: dict[str, int] = {}
    for key, tbl in {
        "fact_trips":      "fact_trips",
        "mart_hourly_kpi": "mart_hourly_kpi",
        "dim_location":    "dim_location",
    }.items():
        for attempt in range(3):
            try:
                dataset_ids[key] = client.get_or_create_dataset(db_id, tbl)
                break
            except Exception as exc:
                log.warning("Dataset '%s' attempt %d failed: %s", tbl, attempt + 1, exc)
                if attempt == 2:
                    log.error("Giving up on dataset '%s'", tbl)
                    raise
                time.sleep(3)

    log.info("Datasets ready: %s", dataset_ids)

    # 4. Charts
    specs = chart_specs(dataset_ids)
    chart_ids: list[int] = []
    failed: list[str] = []

    for spec in specs:
        try:
            chart_ids.append(client.create_chart(spec))
            time.sleep(0.3)
        except Exception as exc:
            log.error("Chart FAILED '%s': %s", spec["slice_name"], exc)
            failed.append(spec["slice_name"])

    log.info(
        "Charts: %d / %d created%s",
        len(chart_ids), len(specs),
        f"  |  FAILED: {failed}" if failed else "",
    )

    if not chart_ids:
        raise RuntimeError("No charts were created — aborting dashboard creation.")

    # 5. Dashboard
    dash_id = client.create_dashboard("NYC TLC Analytics Dashboard", chart_ids)
    log.info("Done → %s/superset/dashboard/%s/", SUPERSET_URL, dash_id)


if __name__ == "__main__":
    main()