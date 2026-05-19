-- models/marts/dim_datetime.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Date/time dimension generated from the actual pickup_date range present
-- in fact_trips.  No external seed required — Athena generates the spine
-- with a sequence() call.
--
-- Grain: one row per calendar date (DATE granularity).
--        Join to fact_trips on fact_trips.pickup_date = dim_datetime.date_day.
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'table',
    schema       = 'marts'
  )
}}

with date_spine as (
    -- Generate one row per day between the project start date and today.
    -- sequence() returns an ARRAY; unnest() expands it to rows.
    select
        cast(date_val as date) as date_day
    from unnest(
        sequence(
            date '{{ var("start_date") }}',
            current_date,
            interval '1' day
        )
    ) as t(date_val)
),

enriched as (
    select
        date_day,

        -- ── Calendar attributes ──────────────────────────────────────────────
        year(date_day)                          as year,
        month(date_day)                         as month,
        day(date_day)                           as day_of_month,
        day_of_week(date_day)                   as day_of_week,   -- 1=Sunday … 7=Saturday
        week_of_year(date_day)                  as week_of_year,
        quarter(date_day)                       as quarter,

        -- ── Human-readable labels ────────────────────────────────────────────
        date_format(date_day, '%Y-%m')          as year_month,
        date_format(date_day, '%A')             as day_name,      -- e.g. Monday
        date_format(date_day, '%B')             as month_name,    -- e.g. January
        date_format(date_day, '%Y-Q')
            || cast(quarter(date_day) as varchar) as year_quarter, -- e.g. 2023-Q1

        -- ── Weekend / weekday flag ───────────────────────────────────────────
        case
            when day_of_week(date_day) in (1, 7) then true
            else false
        end                                     as is_weekend,

        -- ── US federal holidays (fixed-date only; approximate) ───────────────
        case
            when month(date_day) = 1  and day(date_day) = 1  then "New Year''s Day"
            when month(date_day) = 7  and day(date_day) = 4  then 'Independence Day'
            when month(date_day) = 11 and day(date_day) = 11 then "Veterans Day"
            when month(date_day) = 12 and day(date_day) = 25 then 'Christmas Day'
            else null
        end                                     as us_holiday_name,

        case
            when month(date_day) = 1  and day(date_day) = 1  then true
            when month(date_day) = 7  and day(date_day) = 4  then true
            when month(date_day) = 11 and day(date_day) = 11 then true
            when month(date_day) = 12 and day(date_day) = 25 then true
            else false
        end                                     as is_us_holiday

    from date_spine
)

select * from enriched
order by date_day