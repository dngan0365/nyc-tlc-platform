-- models/staging/stg_yellow_trips.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Thin view over the silver-zone yellow taxi table written by job1_cleanse.py.
-- Renames columns to the project-standard snake_case convention and casts
-- types so downstream models don't need to repeat the same expressions.
--
-- Source: nyc_tlc_silver.yellow (Glue catalog, S3 silver/yellow/)
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}

select
    -- ── Identifiers ─────────────────────────────────────────────────────────
    cast(vendorid          as integer)          as vendor_id,
    cast(ratecodeid        as integer)          as rate_code_id,
    cast(pulocationid      as integer)          as pickup_location_id,
    cast(dolocationid      as integer)          as dropoff_location_id,
    cast(payment_type      as integer)          as payment_type_id,

    -- ── Trip flags ───────────────────────────────────────────────────────────
    cast(store_and_fwd_flag as varchar)         as store_and_fwd_flag,
    cast(passenger_count   as integer)          as passenger_count,

    -- ── Timestamps ───────────────────────────────────────────────────────────
    cast(tpep_pickup_datetime  as timestamp)    as pickup_at,
    cast(tpep_dropoff_datetime as timestamp)    as dropoff_at,
    date(tpep_pickup_datetime)                  as pickup_date,    -- partition key
    hour(cast(tpep_pickup_datetime as timestamp)) as pickup_hour,

    -- ── Distance & duration ──────────────────────────────────────────────────
    cast(trip_distance     as double)           as trip_distance_miles,
    date_diff(
        'second',
        cast(tpep_pickup_datetime  as timestamp),
        cast(tpep_dropoff_datetime as timestamp)
    )                                           as trip_duration_seconds,

    -- ── Financials ───────────────────────────────────────────────────────────
    cast(fare_amount       as double)           as fare_amount,
    cast(extra             as double)           as extra,
    cast(mta_tax           as double)           as mta_tax,
    cast(tip_amount        as double)           as tip_amount,
    cast(tolls_amount      as double)           as tolls_amount,
    cast(improvement_surcharge as double)       as improvement_surcharge,
    cast(total_amount      as double)           as total_amount,
    cast(congestion_surcharge  as double)       as congestion_surcharge,
    cast(airport_fee       as double)           as airport_fee,

    -- ── Metadata ─────────────────────────────────────────────────────────────
    'yellow'                                    as cab_type

from {{ source('silver', 'yellow') }}

where
    -- Basic sanity gates (hard rejects should have been quarantined upstream,
    -- but a second check here is cheap and keeps staging clean)
    tpep_pickup_datetime  is not null
    and tpep_dropoff_datetime is not null
    and tpep_pickup_datetime < tpep_dropoff_datetime
    and cast(total_amount as double) >= 0