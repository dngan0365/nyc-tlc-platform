-- models/staging/stg_green_trips.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Thin view over the silver-zone green taxi table.
-- Green trips use lpep_* timestamp columns and have no airport_fee field.
-- All column names are normalised to match stg_yellow_trips so the
-- intermediate union model can combine them without CASE expressions.
--
-- Source: nyc_tlc_silver.green (Glue catalog, S3 silver/green/)
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
    cast(lpep_pickup_datetime  as timestamp)    as pickup_at,
    cast(lpep_dropoff_datetime as timestamp)    as dropoff_at,
    date(lpep_pickup_datetime)                  as pickup_date,
    hour(cast(lpep_pickup_datetime as timestamp)) as pickup_hour,

    -- ── Distance & duration ──────────────────────────────────────────────────
    cast(trip_distance     as double)           as trip_distance_miles,
    date_diff(
        'second',
        cast(lpep_pickup_datetime  as timestamp),
        cast(lpep_dropoff_datetime as timestamp)
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
    cast(null as double)                        as airport_fee,   -- not in green dataset

    -- ── Metadata ─────────────────────────────────────────────────────────────
    'green'                                     as cab_type

from {{ source('silver', 'green') }}

where
    lpep_pickup_datetime  is not null
    and lpep_dropoff_datetime is not null
    and lpep_pickup_datetime < lpep_dropoff_datetime
    and cast(total_amount as double) >= 0