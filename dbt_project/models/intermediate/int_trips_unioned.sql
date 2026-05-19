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
)

select
    -- ── Surrogate key ────────────────────────────────────────────────────────
    -- Deterministic hash so re-runs produce the same key value.
    -- Athena uses xxhash64; adjust to md5() if your Athena engine version
    -- doesn't support xxhash64.
    to_hex(xxhash64(to_utf8(
        concat(
            cast(pickup_at            as varchar), '|',
            cast(pickup_location_id   as varchar), '|',
            cast(dropoff_location_id  as varchar), '|',
            cab_type
        )
    )))                                         as trip_key,

    *

from unioned