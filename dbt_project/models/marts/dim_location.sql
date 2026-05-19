-- models/marts/dim_location.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Taxi zone dimension table.
-- Covers all 263 official TLC taxi zones plus zone 264 (Unknown) and
-- zone 265 (N/A) that appear in trip records.
--
-- Source: the taxi_zone_lookup CSV is loaded as a dbt seed
--         (`dbt seed --select taxi_zone_lookup`) and lives in
--         seeds/taxi_zone_lookup.csv.
--
-- Grain: one row per location_id (1–265).
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'table',
    schema       = 'marts'
  )
}}

with zones as (
    select
        locationid      as location_id,
        borough,
        zone            as zone_name,
        service_zone
    from {{ ref('taxi_zone_lookup') }}
),

-- Add a small set of derived convenience columns used frequently in
-- Superset charts and dbt downstream models.
enriched as (
    select
        location_id,
        zone_name,
        borough,
        service_zone,

        -- Borough grouping for high-level dashboards
        case borough
            when 'Manhattan'    then 'Manhattan'
            when 'Brooklyn'     then 'Outer boroughs'
            when 'Queens'       then 'Outer boroughs'
            when 'Bronx'        then 'Outer boroughs'
            when 'Staten Island' then 'Outer boroughs'
            else 'Unknown / EWR'
        end                         as borough_group,

        -- Is this zone inside one of the major airports?
        case
            when zone_name like '%JFK%'    then true
            when zone_name like '%LaGuardia%' then true
            when zone_name like '%Newark%' then true
            else false
        end                         as is_airport,

        -- Convenience flag for Manhattan CBD (zones 4, 12, 13, 24, 41–45, etc.)
        -- Approximated by borough + service_zone here; refine with exact IDs
        -- if you need precise CBD boundaries.
        case
            when borough = 'Manhattan' and service_zone = 'Yellow Zone'
            then true
            else false
        end                         as is_manhattan_cbd

    from zones
)

select * from enriched