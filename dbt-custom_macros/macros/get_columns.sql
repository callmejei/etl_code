{%-macro get_columns(db,ldg_tbl) -%}
    {% set my_relation = adapter.get_relation(
    database=db,
    schema= db, 
    identifier= ldg_tbl) %}
    {%-set columns = adapter.get_columns_in_relation(my_relation)|map(attribute='column')|join(', ') -%}
    {{return(columns)}}
{% endmacro %}


