{% macro parse_dbt_results(run_result) %}

    {# Flatten results and add to list #}
    {# Convert the run result object to a simple dictionary #}
    {% set run_result_dict = run_result.to_dict() %}
    {% set node = run_result_dict.get('node') %}
    {% set schema_name = node.get('schema') | upper | replace('.', '_') %}
    {% set name = node.get('name') %}
    {% set txf_pkg_name = 'PKG_' + schema_name + name | upper | replace('.', '_') %}
    {% set bizdate = var('businessdate') %}

    {% if run_result_dict['node']['config']['target_table'] %}
        {% set target_table = run_result_dict['node']['config']['target_table'] %}
        {% set ldg_tbl = target_table %}
    {% else %}
        {% set ldg_tbl = name %}
    {% endif %}
    {% if target.type == 'trino' %}
        {% if node.config.get('target_schema') %}
            {% set db = target.database + node.config.get('target_schema') %}
            {% set catalog = target.database %}
        {% else %}
            {% set db = target.database + node.get('schema') %}
            {% set catalog = target.database %}
        {% endif %}
    {% else %}
        {% set db = node.get('schema') %}
        {% set catalog = 'NA' %}
    {% endif %}

    {% set txf_batch_id = var('batchid') %}
    {% set userid = target.user %}

    {# Message Generation #}
    {% set dict_message = run_result_dict.get('message') %}
    {% if dict_message == None %}
        {% set message = dict_message %}
    {% else %}
        {% set message = dict_message.replace("", "") %}
    {% endif %}
    
    {% set partition_flag = node['config']['partition_flag'] %}
    {% set materialized = node['config']['materialized'] %}


    {# Rows Affected #}
    {% if node.get('resource_type') == 'model' and run_result_dict.get('status') == 'success' %}
            {% set target_columns_func = custom_macros.get_columns(db,ldg_tbl) %}
            {% call statement('count_model_fetch_result', True) %}
                {% if 'businessdate' in target_columns_func and 'ins_txf_batchid' in target_columns_func %}
                    SELECT count(*) as count from {{ db }}.{{ ldg_tbl }}
                    WHERE businessdate='{{ bizdate }}' AND ins_txf_batchid='{{ var('batchid') }}'
                {% elif 'businessdate' in target_columns_func %}
                    SELECT count(*) as count from {{ db }}.{{ ldg_tbl }}
                    WHERE businessdate='{{ bizdate }}'
                {% elif 'ins_txf_batchid' in target_columns_func %}
                    SELECT count(*) as count from {{ db }}.{{ ldg_tbl }}
                    WHERE ins_txf_batchid='{{ var('batchid') }}'
                {% else %}
                    {% set rows_affected = '0' %}
                {% endif %}
            {% endcall %}
            {% set rows_affected = load_result('count_model_fetch_result')['data'][0][0] %}
    {% else %}
	
	        {% set rows_affected = '0' %}
    {% endif %}

    {# Query ID #}
    {% set query_id = run_result_dict.get('adapter_response', {}).get('query_id', 'NA') %}

    {# Compiled Code #}
    {% if node['compiled_code'] %}
        {{ log("Compiled code for model " ~ node['compiled_code'], True) }}
    {% endif %}

    {# Timing #}
    {% if run_result_dict.get('timing') %}
        {% set started_at = run_result_dict.get('timing')[0].get('started_at', 0) %}
        {% set completed_at = run_result_dict.get('timing')[-1].get('completed_at', 0) %}
    {% else %}
        {% set started_at_cast = "null" %}
        {% set completed_at_cast = "null" %}
    {% endif %}

    {% set parsed_result_dict = {
        'result_id': invocation_id ~ node.get('unique_id'),
        'invocation_id': invocation_id,
        'unique_id': node.get('unique_id'),
        'txf_pkg_name': txf_pkg_name,
        'txf_batch_id': txf_batch_id,
        'biz_date': bizdate,
        'catalog_name': catalog,
        'schema_name': node.get('schema'),
        'table_name': ldg_tbl,
        'status': run_result_dict.get('status'),
        'started_at': started_at_cast,
        'completed_at': completed_at_cast,
        'execution_time': run_result_dict.get('execution_time'),
        'rows_affected': rows_affected,
        'message': message,
        'userid': userid,
        'query_id': query_id
    } %}

    {{ return(parsed_result_dict) }}

{% endmacro %}
