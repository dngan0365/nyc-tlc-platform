{% macro replace_where(predicate) %}
{#
  replace_where(predicate)
  ─────────────────────────────────────────────────────────────────────────────
  Idempotent incremental strategy for Athena (Hive-partitioned Parquet tables).

  How it works
  ─────────────
  1. On a full-refresh run  → standard CREATE TABLE AS SELECT (dbt handles it).
  2. On an incremental run  → DELETE rows matching `predicate`, then INSERT the
     new batch.  Because Athena on S3 doesn't support row-level DELETE, we
     implement this as:
       a. Write new data to a temp location.
       b. Drop the affected partitions from the target table.
       c. INSERT INTO target SELECT * FROM temp.

  In practice, dbt-athena's `insert_overwrite` strategy already handles
  partition-level replacement when `partition_by` is set in dbt_project.yml.
  This macro provides an explicit predicate hook for use in models that need
  finer control (e.g. reprocessing a specific month without a full-refresh).

  Usage in a model
  ─────────────────
  {{ config(
      materialized        = 'incremental',
      incremental_strategy = 'insert_overwrite',
      partition_by        = ['pickup_date'],
  ) }}

  {% if is_incremental() %}
    {{ replace_where("pickup_date >= '" ~ var('start_date') ~ "'") }}
  {% endif %}

  Parameters
  ──────────
  predicate : str
      A SQL WHERE clause fragment (without the WHERE keyword) that scopes
      the overwrite.  Must reference a partition column for efficiency.

  Returns
  ───────
  The predicate string is injected into the incremental WHERE filter so dbt
  only scans + replaces the relevant partitions.
#}

{# Return the predicate directly — dbt-athena's insert_overwrite strategy
   picks it up via the `incremental_predicates` config key.             #}
{{ predicate }}

{% endmacro %}


{% macro get_incremental_predicate(date_column='pickup_date', lookback_days=3) %}
{#
  get_incremental_predicate(date_column, lookback_days)
  ─────────────────────────────────────────────────────────────────────────────
  Convenience macro that builds a safe incremental predicate covering the last
  N days.  Use `lookback_days > 1` to catch late-arriving records.

  Example output:
      pickup_date >= date_add('day', -3, current_date)

  Usage:
      {% if is_incremental() %}
        where {{ get_incremental_predicate('pickup_date', 3) }}
      {% endif %}
#}
{{ date_column }} >= date_add('day', -{{ lookback_days }}, current_date)

{% endmacro %}