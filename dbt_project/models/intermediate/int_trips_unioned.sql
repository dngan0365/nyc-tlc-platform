-- models/intermediate/int_trips_unioned.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Unions yellow and green staging views into a single normalised trip stream.
-- All column names and types are already aligned in the staging layer so this
-- model is a straight UNION ALL — no casting or renaming needed here.
--
-- A surrogate trip key is derived from the pickup timestamp, location IDs,
-- and cab type.  This is the dedup key used by fact_trips.
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'view',
    schema       = 'intermediate'
  )
}}

with yellow as (
    select * from {{ ref('stg_yellow_trips') }}
),

green as (
    select * from {{ ref('stg_green_trips') }}
),

unioned as (
    select * from yellow
    union all
    select * from green
),

keyed as (
    select
        -- ── Surrogate key ────────────────────────────────────────────────
        -- Five semantically meaningful fields + a row_number tiebreaker.
        -- row_number is 1 for all truly unique trips, so it doesn't bloat
        -- the key for the happy path — it only fires for genuine dupes.
        to_hex(xxhash64(to_utf8(
            concat(
                cast(pickup_at           as varchar), '|',
                cast(dropoff_at          as varchar), '|',
                cast(pickup_location_id  as varchar), '|',
                cast(dropoff_location_id as varchar), '|',
                cast(fare_amount         as varchar), '|',
                cab_type,                             '|',
                cast(
                    row_number() over (
                        partition by
                            pickup_at,
                            dropoff_at,
                            pickup_location_id,
                            dropoff_location_id,
                            fare_amount,
                            cab_type
                        order by
                            total_amount,   -- secondary: prefer higher-value row
                            tip_amount      -- tertiary: then higher tip
                    ) as varchar)
            )
        )))                                     as trip_id,

        *

    from unioned
)

select * from keyed