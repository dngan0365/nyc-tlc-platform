-- models/marts/fact_trips.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Incremental fact table for individual taxi trips.
--
-- Materialisation strategy
-- ─────────────────────────
-- • insert_overwrite on the pickup_date partition column.
-- • On each run, dbt rewrites only partitions within the incremental
--   predicate window (last 3 days by default, to catch late arrivals).
-- • Full-refresh (`dbt run --full-refresh`) rebuilds the entire table.
--
-- Grain: one row per trip (trip_id is the unique identifier).
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
    select * from {{ ref('int_trips_unioned') }}
),

-- Payment type dimension (inline — small enough to not need a separate table)
payment_types as (
    select *
    from (values
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')
    ) as t (payment_type_id, payment_type_desc)
),

-- Rate code dimension (inline)
rate_codes as (
    select *
    from (values
        (1, 'Standard rate'),
        (2, 'JFK'),
        (3, 'Newark'),
        (4, 'Nassau or Westchester'),
        (5, 'Negotiated fare'),
        (6, 'Group ride')
    ) as t (rate_code_id, rate_code_desc)
),

enriched as (
    select
        -- ── Keys ─────────────────────────────────────────────────────────────
        t.trip_id,
        t.pickup_location_id,
        t.dropoff_location_id,

        -- ── Timestamps (FK → dim_datetime) ───────────────────────────────────
        t.pickup_at,
        t.dropoff_at,
        t.pickup_date,       -- partition column — keep as DATE for efficiency
        t.pickup_hour,

        -- ── Descriptors ──────────────────────────────────────────────────────
        t.cab_type,
        t.vendor_id,
        t.passenger_count,
        t.store_and_fwd_flag,
        coalesce(pt.payment_type_desc, 'Unknown')   as payment_type,
        coalesce(rc.rate_code_desc,    'Unknown')   as rate_code,

        -- ── Trip metrics ─────────────────────────────────────────────────────
        t.trip_distance_miles,
        t.trip_duration_seconds,
        round(t.trip_distance_miles / nullif(t.trip_duration_seconds / 3600.0, 0), 2)
                                                    as avg_speed_mph,

        -- ── Financial metrics ────────────────────────────────────────────────
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.airport_fee,
        t.total_amount,

        -- Derived: tip as a percentage of fare (null-safe)
        case
            when t.fare_amount > 0
            then round(t.tip_amount / t.fare_amount * 100, 2)
            else null
        end                                         as tip_pct,

        -- Derived: per-mile revenue
        case
            when t.trip_distance_miles > 0
            then round(t.total_amount / t.trip_distance_miles, 2)
            else null
        end                                         as revenue_per_mile

    from trips t
    left join payment_types pt using (payment_type_id)
    left join rate_codes     rc using (rate_code_id)
)

select * from enriched

-- ── Incremental filter ───────────────────────────────────────────────────────
-- On incremental runs, restrict to the last 3 days so late-arriving records
-- are picked up without reprocessing the full history.
{% if is_incremental() %}
where {{ get_incremental_predicate('pickup_date', lookback_days=3) }}
{% endif %}