{{
    config(
        materialized='view',
        tags=['intermediate', 'target_disease']
    )
}}

WITH target_data AS (
    SELECT
        id as target_id,
        approved_name as target_name,
        approved_symbol as target_symbol,
        biotype
    FROM {{ ref('stg_targets') }}
),

disease_data AS (
    SELECT
        id as disease_id,
        name as disease_name,
        description as disease_description,
        therapeutic_areas
    FROM {{ ref('stg_diseases') }}
),

association_data AS (
    SELECT
        target_id,
        disease_id,
        score_overall as association_score,
        score_direct,
        score_indirect,
        datasource_scores
    FROM {{ ref('stg_associations') }}
)

SELECT
    a.target_id,
    a.disease_id,
    t.target_name,
    t.target_symbol,
    t.biotype,
    d.disease_name,
    d.disease_description,
    d.therapeutic_areas,
    a.association_score,
    a.score_direct,
    a.score_indirect,
    a.datasource_scores,
    CURRENT_TIMESTAMP() as processed_at
FROM association_data a
LEFT JOIN target_data t ON a.target_id = t.target_id
LEFT JOIN disease_data d ON a.disease_id = d.disease_id
