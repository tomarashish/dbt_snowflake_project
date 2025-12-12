{{
    config(
        materialized='table',
        tags=['marts', 'target_disease_associations']
    )
}}

WITH target_disease_data AS (
    SELECT
        target_id,
        disease_id,
        target_name,
        target_symbol,
        biotype,
        disease_name,
        disease_description,
        therapeutic_areas,
        association_score,
        score_direct,
        score_indirect,
        datasource_scores
    FROM {{ ref('int_target_disease') }}
),

evidence_summary AS (
    SELECT
        target_id,
        disease_id,
        COUNT(*) as evidence_count,
        MAX(score) as max_evidence_score,
        AVG(score) as avg_evidence_score,
        ARRAY_AGG(DISTINCT datasource) as evidence_sources,
        ARRAY_AGG(OBJECT_CONSTRUCT('datasource', datasource, 'score', score, 'date', date)) as evidence_details
    FROM {{ ref('stg_evidence') }}
    GROUP BY target_id, disease_id
)

SELECT
    td.target_id,
    td.disease_id,
    td.target_name,
    td.target_symbol,
    td.biotype,
    td.disease_name,
    td.disease_description,
    td.therapeutic_areas,
    td.association_score,
    td.score_direct,
    td.score_indirect,
    td.datasource_scores,
    COALESCE(es.evidence_count, 0) as evidence_count,
    es.max_evidence_score,
    es.avg_evidence_score,
    es.evidence_sources,
    es.evidence_details,
    CURRENT_TIMESTAMP() as processed_at
FROM target_disease_data td
LEFT JOIN evidence_summary es ON td.target_id = es.target_id AND td.disease_id = es.disease_id
