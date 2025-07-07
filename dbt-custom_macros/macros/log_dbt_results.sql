{%-macro log_dbt_results(results) -%}
    {{print(results)}}
    {%-set parsed_results = [] -%}
    {%-set parsed_test_results = [] -%}
    {%-set parsed_dqmf_results = [] -%}
    {%-for run_result in results -%}
        {%-set run_result_dict = run_result.to_dict() -%}

        {%-set node = run_result_dict.get('node') -%}
        {%-set audit_flag = node.config.get('enable_audit')|upper -%}
        {%-set resource_type = node.get('resource_type') -%}
        {%-set model_name = node.get('name') -%}

        {%-if audit_flag|upper == 'FALSE' -%}
            {{ print("Audit capture disabled for Model "+model_name) }}
        {%-elif resource_type == 'model' and audit_flag|upper != 'FALSE' -%}    
            {%-set parsed_result_dict = custom_macros.parse_dbt_results(run_result) -%}
            {%-do parsed_results.append(parsed_result_dict) -%}
        {%-elif resource_type == 'test' and audit_flag|upper != 'FALSE' -%}
            {%-set parsed_test_result_dict = custom_macros.parse_dbt_test_results(run_result) -%}
            {%-set parsed_dqmf_result_dict = custom_macros.parse_dqmf_results(run_result) -%}
            {%-do parsed_test_results.append(parsed_test_result_dict) -%}
            {%-do parsed_dqmf_results.append(parsed_dqmf_result_dict) -%}
        {%-endif -%}
    {%-endfor -%}
    
    {%- if target.name == 'dev' -%}
        {%-set db = target.schema -%}
    {%- elif target.name == 'uat' -%}
        {%-set db = 'hdu4_custom_macros' -%}
    {%- elif target.name == 'prod' -%}
        {%-set db = 'hdp_custom_macros' -%}
    {%- endif -%}

{%-set insert_dbt_results_query -%}
insert overwrite table {{db}}.custom_macros_run_results partition(businessdate='{{ var("businessdate") }}')
values
{%-for parsed_result_dict in parsed_results -%}
(
    '{{ parsed_result_dict.get('invocation_id') }}',
    '{{ parsed_result_dict.get('txf_pkg_name') }}',
    '{{ parsed_result_dict.get('txf_batch_id') }}',
    '{{ parsed_result_dict.get('userid') }}',
    '{{ parsed_result_dict.get('catalog_name') }}',
    '{{ parsed_result_dict.get('schema_name') }}',
    '{{ parsed_result_dict.get('table_name') }}',
    '{{ parsed_result_dict.get('status') }}',
    '{{ parsed_result_dict.get('started_at') }}',
    '{{ parsed_result_dict.get('completed_at') }}',
    '{{ parsed_result_dict.get('execution_time') }}',
    '{{ parsed_result_dict.get('rows_affected') }}',
    '{{ parsed_result_dict.get('message') }}'
) {{ "," if not loop.last else "" }}
{%-endfor -%}
{%-endset -%}
{{print(insert_dbt_results_query)}}

{%-set insert_dbt_test_results_query -%}
insert into {{db}}.custom_macros_dq_results
(
    unique_id,
    userid,
    catalog_name,
    table_name,
    column_name,
    missing
) values
{%- for parsed_test_result_dict in parsed_test_results -%}
(
    '{{ parsed_test_result_dict.get('unique_id') }}',
    '{{ parsed_test_result_dict.get('userid') }}',
    '{{ parsed_test_result_dict.get('catalog_name') }}',
    '{{ parsed_test_result_dict.get('table_name') }}',
    '{{ parsed_test_result_dict.get('column_name') }}',
    '{{ parsed_test_result_dict.get('test_case') }}',
    {{ parsed_test_result_dict.get('started_at') }},
    {{ parsed_test_result_dict.get('completed_at') }},
    '{{ parsed_test_result_dict.get('execution_time') }}',
    '{{ parsed_test_result_dict.get('status') }}',
    '{{ parsed_test_result_dict.get('message') }}',
    '{{ parsed_test_result_dict.get('biz_date') }}'
) {{ "," if not loop.last else "" }}
{%- endfor -%}
{%-endset -%}

{%-set insert_dqmf_test_results_query -%}
insert into {{db}}.custom_macros_dqmf_results
(
    invocation_id
    ,userid
    ,project_name
    ,test_unique_id
    ,test_execution_id
    ,model_unique_id
    ,started_at
    ,completed_at
    ,execution_time
    ,database_name
    ,schema_name
    ,table_name
    ,column_name
    ,test_name
    ,test_params
    ,test_results_description
    ,test_results_query
    ,failures
    ,severity
    ,status
    ,failed_row_count
    ,biz_date
) values
{%- for parsed_dqmf_result_dict in parsed_dqmf_results -%}
(
    '{{ parsed_dqmf_result_dict.get('invocation_id') }}'
    ,'{{ parsed_dqmf_result_dict.get('userid') }}'
    ,'{{ parsed_dqmf_result_dict.get('project_name') }}'
    ,'{{ parsed_dqmf_result_dict.get('test_unique_id') }}'
    ,'{{ parsed_dqmf_result_dict.get('test_execution_id') }}'
    ,'{{ parsed_dqmf_result_dict.get('model_unique_id') }}'
    ,{{ parsed_dqmf_result_dict.get('started_at') }},
    ,{{ parsed_dqmf_result_dict.get('completed_at') }},
    ,'{{ parsed_dqmf_result_dict.get('execution_time') }}'
    ,'{{ parsed_dqmf_result_dict.get('database_name') }}'
    ,'{{ parsed_dqmf_result_dict.get('schema_name') }}'
    ,'{{ parsed_dqmf_result_dict.get('table_name') }}'
    ,'{{ parsed_dqmf_result_dict.get('column_name') }}'
    ,'{{ parsed_dqmf_result_dict.get('test_name') }}'
    ,'{{ parsed_dqmf_result_dict.get('test_params') }}'
    ,'{{ parsed_dqmf_result_dict.get('test_results_description') }}'
    ,'{{ parsed_dqmf_result_dict.get('test_results_query') }}'
    ,'{{ parsed_dqmf_result_dict.get('failures') }}'
    ,'{{ parsed_dqmf_result_dict.get('severity') }}'
    ,'{{ parsed_dqmf_result_dict.get('status') }}'
    ,{{ parsed_dqmf_result_dict.get('failed_row_count') }},
    ,'{{ parsed_dqmf_result_dict.get('biz_ date') }}'
    {{- "," if not loop.last else "" -}}
{%-endfor -%}
{%-endset -%}

{%-set parsed_results_len = parsed_results | length -%}
{%-set parsed_test_results_len = parsed_test_results | length -%}

{%-if parsed_results_len != 0 and parsed_test_results_len != 0 -%}
    {%-do run_query(insert_dbt_results_query) -%}
    {%-do run_query(insert_dbt_test_results_query) -%}
    {%-do run_query(insert_dqmf_test_results_query) -%}
    {{ return ('') }}
{%-elif parsed_test_results_len != 0 -%}
    {%-do run_query(insert_dbt_test_results_query) -%}
    {%-do run_query(insert_dqmf_test_results_query) -%}
    {{ return ('') }}
{%-elif parsed_results_len != 0 -%}
    {%-do run_query(insert_dbt_results_query) -%}
    {{ return ('') }}
{%-endif -%}

{%-endmacro -%}
