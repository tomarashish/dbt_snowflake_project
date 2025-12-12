{{
    config(
        materialized='view',
        tags=['staging', 'diseases']
    )
}}

WITH source_data AS (
    SELECT
        $1:id::STRING as id,
        $1:name::STRING as name,
        $1:description::STRING as description,
        $1:therapeuticAreas::ARRAY as therapeutic_areas,
        $1:dbXRefs::VARIANT as db_xrefs,
        $1:synonyms::ARRAY as synonyms,
        $1:ancestors::ARRAY as ancestors,
        $1:descendants::ARRAY as descendants,
        $1:children::ARRAY as children,
        $1:parents::ARRAY as parents,
        $1 as raw_data,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @{{ var('opentarget_stage') }}/diseases/*.parquet
)

SELECT
    id,
    name,
    description,
    therapeutic_areas,
    db_xrefs,
    synonyms,
    ancestors,
    descendants,
    children,
    parents,
    raw_data,
    loaded_at
FROM source_data
