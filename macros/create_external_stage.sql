{% macro create_external_stage(stage_name, url_path, file_format_name='parquet_format') %}

    {% set create_file_format_query %}
    CREATE FILE FORMAT IF NOT EXISTS {{ file_format_name }}
        TYPE = 'PARQUET'
        COMPRESSION = 'AUTO';
    {% endset %}
    
    {% do run_query(create_file_format_query) %}
    
    {% set create_stage_query %}
    CREATE STAGE IF NOT EXISTS {{ stage_name }}
        URL = '{{ url_path }}'
        FILE_FORMAT = {{ file_format_name }};
    {% endset %}
    
    {% do run_query(create_stage_query) %}
    
    {% set grant_usage_query %}
    GRANT USAGE ON STAGE {{ stage_name }} TO ROLE {{ target.role }};
    {% endset %}
    
    {% do run_query(grant_usage_query) %}
    
    {{ log("Created external stage " ~ stage_name ~ " pointing to " ~ url_path, info=True) }}
    
{% endmacro %}
