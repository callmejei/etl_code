{% macro yml_parser() %}

  {% set params = config.get('parameters')  %}
  {% set source_table = params['source_table'] %}
  {% set filter = params['filter']  %}
  {% set group_by = params['group_by'] %}
  {% set metrics = params['metrics'] %}
  {% set joins = params['joins'] %}

  {% set select_cols = [] %}

  {% if group_by | length > 0 %}
    {% for col in group_by %}
      {% do select_cols.append(col) %}
    {% endfor %}
  {% endif %}

  {% if metrics | length > 0 %}
    {% for metric in metrics %}
      {% set metric_sql = metric.function ~ '(' ~ metric.column ~ ') AS ' ~ metric.name %}
      {% do select_cols.append(metric_sql) %}
    {% endfor %}
  {% endif %}

  {% if select_cols | length == 0 %}
    {% do select_cols.append('*') %}
  {% endif %}

  {% set select_sql = select_cols | join(', ') %}

  {% set join_sqls = [] %}
  {% for join in joins %}
    {% set join_sql = 'JOIN ' ~ join.table ~ ' ON ' ~ join.on_cond %}
    {% do join_sqls.append(join_sql) %}
  {% endfor %}
  {% set join_sql_final = join_sqls | join(' ') %}

  {% if filter is not none %}
    {% set where_sql = 'WHERE ' ~ filter %}
  {% else %}
    {% set where_sql = '' %}
  {% endif %}

  {% if group_by | length > 0 %}
    {% set group_by_sql = 'GROUP BY ' ~ (group_by | join(', ')) %}
  {% else %}
    {% set group_by_sql = '' %}
  {% endif %}

  -- Final SQL
  SELECT
    {{ select_sql }}
  FROM {{ source_table }}
  {{ join_sql_final }}
  {{ where_sql }}
  {{ group_by_sql }}


{% endmacro %}
