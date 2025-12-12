{{
    config(
        materialized='view',
        tags=['staging', 'associations']
    )
}}

WITH source_data AS (
    SELECT
        $1:target::VARIANT:id::STRING as target_id,
        $1:disease::VARIANT:id::STRING as disease_id,
        $1:datasourceScores::VARIANT as datasource_scores,
        $1:scoresByDatatype::VARIANT as scores_by_datatype,
        $1:scoresByDataSource::VARIANT as scores_by_datasource,
        $1:overallScore::FLOAT as score_overall,
        $1:directScore::FLOAT as score_direct,
        $1:indirectScore::FLOAT as score_indirect,
        $1 as raw_data,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @{{ var('opentarget_stage') }}/associations/*.parquet
)

SELECT
    target_id,
    disease_id,
    datasource_scores,
    scores_by_datatype,
    scores_by_datasource,
    score_overall,
    score_direct,
    score_indirect,
    raw_data,
    loaded_at
FROM source_data
