{% macro load_parquet_to_table(stage_name, table_name, pattern='.*\.parquet', columns=None) %}

    {% set create_table_query %}
    CREATE TABLE IF NOT EXISTS {{ table_name }} (
        {% if columns %}
            {% for column in columns %}
                {{ column.name }} {{ column.type }}{% if not loop.last %},{% endif %}
            {% endfor %}
        {% else %}
            raw_data VARIANT
        {% endif %}
    );
    {% endset %}
    
    {% do run_query(create_table_query) %}
    
    {% set copy_query %}
    COPY INTO {{ table_name }}
    FROM @{{ stage_name }}/
    PATTERN = '{{ pattern }}'
    {% if columns %}
        (
        {% for column in columns %}
            {{ column.name }}{% if not loop.last %},{% endif %}
        {% endfor %}
        )
    {% endif %}
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE'
    PURGE = FALSE;
    {% endset %}
    
    {% do run_query(copy_query) %}
    
    {{ log("Loaded data from stage " ~ stage_name ~ " into table " ~ table_name, info=True) }}
    
{% endmacro %}