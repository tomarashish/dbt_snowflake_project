{{
    config(
        materialized='view',
        tags=['staging', 'evidence']
    )
}}

WITH source_data AS (
    SELECT
        $1:targetId::STRING as target_id,
        $1:diseaseId::STRING as disease_id,
        $1:datasourceId::STRING as datasource,
        $1:datatypeId::STRING as datatype,
        $1:score::FLOAT as score,
        $1:literature::VARIANT as literature,
        $1:evidenceLevel::STRING as evidence_level,
        $1:resourceScore::FLOAT as resource_score,
        $1:date::DATE as date,
        $1 as raw_data,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @{{ var('opentarget_stage') }}/evidence/*/*.parquet
)

SELECT
    target_id,
    disease_id,
    datasource,
    datatype,
    score,
    literature,
    evidence_level,
    resource_score,
    date,
    raw_data,
    loaded_at
FROM source_data
