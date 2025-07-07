{% macro post_proc() %} 
    {% set model_full_name = this | string %}
    {% set db = model_full_name.split('.')[0] %}
    {% set ldg_tbl = model_full_name.split('.')[1] %}
    {% set model_columns = custom_macros.get_columns(db, ldg_tbl) %}

    {% if config.get('target_table') and config.get('target_schema') %}
        {% set ldg_tbl = config.get('target_table') %}
        {% set db = config.get('target_schema') %}
        {% set target_tbl_cols = custom_macros.get_columns(db, ldg_tbl) %}
        {% set cols = [] %}
        {% set infra_cols = ['businessdate_process_dt', 'start_dt', 'end_dt', 'processname', 'ins_txf_batchid', 'u'] %}
        
        {% set model_columns_list = model_columns.split(',') | map('trim') | list %}

        {% for column in target_tbl_cols.split(',') %}
            {% set column = column.strip() %}  
            {% if column in model_columns_list %}
                {% do cols.append(column) %}
            {% elif column not in infra_cols %}
                {% set null_gen = "null as " ~ column %}
                {% do cols.append(null_gen) %}
            {% endif %}
        {% endfor %}

        {% set column_names = cols | join(', ') %}
        {{print("1")}}
        {{print(db)}}
        {{print(ldg_tbl)}}
        
        {% call statement('insert', fetch_result=True) %}
            insert overwrite table {{db}}.{{ ldg_tbl }} select
            {{column_names}}
            from {{ this }}
            {% if config.get('hist_run')|upper != 'TRUE' %}
                {% if 'businessdate' in target_tbl_cols and 'ins_txf_batchid' in target_tbl_cols %}
                    where businessdate='{{ var("businessdate") }}' and ins_txf_batchid='{{ var("batchid") }}'
                {% elif 'businessdate' in target_tbl_cols %}
                    where businessdate='{{ var("businessdate") }}'
                {% elif 'ins_txf_batchid' in target_tbl_cols %}
                    where businessdate='{{ var("businessdate") }}'
                {% endif %}
            {% endif %}
        {% endcall %}
    {% endif %}

    {# Month-end logic (always runs if me_flag is TRUE) #}
    {% set me_flag = config.get('me_flag') %}
    {{print("2")}}
    {{print(db)}}
    {{print(ldg_tbl)}}
    {% if me_flag|upper == 'TRUE' %}
        {% call statement('odate', fetch_result=True) %}
            SELECT cast('{{ var("businessdate") }}' as date) as datecol
        {% endcall %}
        
        {% call statement('nodate', fetch_result=True) %}
            SELECT cast('{{ var("nbusinessdate") }}' as date) as datecol
        {% endcall %}
        
        {% set odate = load_result('odate')['data'][0][0] %}
        {% set ndate = load_result('nodate')['data'][0][0] %}
        {% set fdfmonon = odate.replace(day=1) %}
        {% set fdfmonon = (fdfmonon + modules.datetime.timedelta(days=32)).replace(day=1) %}
        {% set ldfomon = fdfmonon - modules.datetime.timedelta(days=1) %}
        {% set daydiff = (ndate - odate).days %}
        {% set mondiff = ndate.year * 12 + ndate.month - (odate.year * 12 + odate.month) %}
        
        {% if mondiff >= 1 and daydiff > 1 and odate != ldfomon %}
            {% set monthend = ldfomon.strftime("%Y-%m-%d") %}
            {% set ldg_tbl_cols = custom_macros.get_columns(db, ldg_tbl).replace("businessdate", "'" ~ monthend ~ "'") %}
            {% call statement('month_end', fetch_result=True) %}
                insert overwrite table {{db}}.{{ldg_tbl}} select {{ ldg_tbl_cols }} from {{db}}.{{ldg_tbl}} where businessdate='{{ var("businessdate") }}'
            {% endcall %}
        {% else %}
            {% set monthend = "" %}
        {% endif %}
    {% endif %}

    {# 9999_flag logic #}
    {% if var("9999_flag", "")|upper == 'FALSE' %}
        {% set cmd_9999_flag = "FALSE" %}
    {% else %}
        {% set cmd_9999_flag = "TRUE" %}
    {% endif %}

    {% if config.get('9999_flag')|upper == 'TRUE' and cmd_9999_flag == 'TRUE' %}
        {% set ldg_tbl_cols = custom_macros.get_columns(db, ldg_tbl).replace("businessdate", "'9999-12-31' as businessdate") %}
        {% call statement('month_end', fetch_result=True) %}
            insert overwrite table {{db}}.{{ldg_tbl}} select {{ ldg_tbl_cols }} from {{db}}.{{ldg_tbl}} where businessdate='{{ var("businessdate") }}'
        {% endcall %}
    {% endif %}

{% endmacro %}
