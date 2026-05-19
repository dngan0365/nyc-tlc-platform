-- models/marts/mart_hourly_kpi.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Pre-aggregated One Big Table (OBT) at pickup_date × pickup_hour grain.
-- Purpose: power Superset charts without hitting fact_trips (billions of rows)
-- at query time.  All KPIs the dashboard needs are materialised here.
--
-- Refresh: incremental, overwriting only the last 3 days of partitions.
-- Grain:   one row per (pickup_date, pickup_hour, cab_type, borough_group).
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized         = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by         = ['pickup_date'],
    schema               = 'marts'
  )
}}

with trips as (
    select
        f.pickup_date,
        f.pickup_hour,
        f.cab_type,
        f.pickup_location_id,
        f.payment_type,
        f.trip_distance_miles,
        f.trip_duration_seconds,
        f.fare_amount,
        f.tip_amount,
        f.total_amount,
        f.congestion_surcharge,
        f.airport_fee,
        f.passenger_count,
        f.avg_speed_mph,
        f.tip_pct,
        l.borough_group,
        l.is_airport,
        l.is_manhattan_cbd,
        d.is_weekend,
        d.is_us_holiday,
        d.day_name,
        d.month_name,
        d.year_month,
        d.year_quarter
    from {{ ref('fact_trips') }}       f
    left join {{ ref('dim_location') }} l on f.pickup_location_id = l.location_id
    left join {{ ref('dim_datetime') }} d on f.pickup_date         = d.date_day

    {% if is_incremental() %}
    where {{ get_incremental_predicate('f.pickup_date', lookback_days=3) }}
    {% endif %}
),

aggregated as (
    select
        -- ── Dimensions (group-by keys) ────────────────────────────────────────
        pickup_date,
        pickup_hour,
        cab_type,
        borough_group,
        is_airport,
        is_manhattan_cbd,
        is_weekend,
        is_us_holiday,
        day_name,
        month_name,
        year_month,
        year_quarter,

        -- ── Volume KPIs ───────────────────────────────────────────────────────
        count(*)                                        as trip_count,
        sum(passenger_count)                            as total_passengers,

        -- ── Distance KPIs ────────────────────────────────────────────────────
        round(sum(trip_distance_miles), 2)              as total_distance_miles,
        round(avg(trip_distance_miles), 3)              as avg_distance_miles,

        -- ── Duration KPIs ────────────────────────────────────────────────────
        round(avg(trip_duration_seconds) / 60.0, 2)    as avg_duration_minutes,
        round(
            approx_percentile(trip_duration_seconds, 0.5) / 60.0,
            2
        )                                               as median_duration_minutes,

        -- ── Speed KPIs ───────────────────────────────────────────────────────
        round(avg(avg_speed_mph), 2)                    as avg_speed_mph,

        -- ── Revenue KPIs ─────────────────────────────────────────────────────
        round(sum(fare_amount), 2)                      as total_fare,
        round(sum(tip_amount), 2)                       as total_tips,
        round(sum(total_amount), 2)                     as total_revenue,
        round(avg(total_amount), 2)                     as avg_revenue_per_trip,
        round(avg(tip_pct), 2)                          as avg_tip_pct,
        round(sum(congestion_surcharge), 2)             as total_congestion_surcharge,
        round(sum(airport_fee), 2)                      as total_airport_fee,

        -- ── Payment mix ──────────────────────────────────────────────────────
        count_if(payment_type = 'Credit card')          as credit_card_trips,
        count_if(payment_type = 'Cash')                 as cash_trips,

        -- ── Rolling window helpers ────────────────────────────────────────────
        -- 7-day and 28-day rolling averages are computed in Superset or via a
        -- separate dbt model using Athena window functions.  We expose the raw
        -- daily grain here so Superset can window over it without re-scanning
        -- fact_trips.
        round(sum(total_amount) / nullif(count(*), 0), 2) as revenue_per_trip

    from trips
    group by
        pickup_date,
        pickup_hour,
        cab_type,
        borough_group,
        is_airport,
        is_manhattan_cbd,
        is_weekend,
        is_us_holiday,
        day_name,
        month_name,
        year_month,
        year_quarter
)

select * from aggregated