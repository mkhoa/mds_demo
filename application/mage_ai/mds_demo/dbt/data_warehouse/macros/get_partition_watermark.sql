{#
  get_partition_watermark(relation)

  Returns a dict {year, month, day} representing the highest Hive partition
  already present in `relation`. Called at the start of an incremental stg model
  to build the partition_filter string that is embedded into the DuckDB scan.

  Defaults to (1900, 1, 1) when the table is empty so the first incremental
  run behaves like a full scan.

  Example:
    {% set wm = get_partition_watermark(this) %}
    {% set filter = '(year, month, day) >= (%s, %s, %s)' % (wm.year, wm.month, wm.day) %}
#}

{% macro get_partition_watermark(relation) %}
    {% set sql %}
        SELECT
            COALESCE(MAX(year),  1900) AS max_year,
            COALESCE(MAX(month),    1) AS max_month,
            COALESCE(MAX(day),      1) AS max_day
        FROM {{ relation }}
    {% endset %}
    {% set result = run_query(sql) %}
    {% set row = result.rows[0] %}
    {{ return({'year': row['max_year'], 'month': row['max_month'], 'day': row['max_day']}) }}
{% endmacro %}
