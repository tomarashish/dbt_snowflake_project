{{
    config(
        materialized='view',
        tags=['staging', 'targets']
    )
}}

WITH source_data AS (
    SELECT
        $1:id::STRING as id,
        $1:approvedName::STRING as approved_name,
        $1:approvedSymbol::STRING as approved_symbol,
        $1:biotype::STRING as biotype,
        $1:genomicLocation::VARIANT as genomic_location,
        $1:proteinAnnotations::VARIANT as protein_annotations,
        $1:tractability::VARIANT as tractability,
        $1:safetyLiabilities::VARIANT as safety_liabilities,
        $1:chemicalProbes::VARIANT as chemical_probes,
        $1:hallmarks::VARIANT as hallmarks,
        $1:synonyms::ARRAY as synonyms,
        $1:obsoleteNames::ARRAY as obsolete_names,
        $1:obsoleteSymbols::ARRAY as obsolete_symbols,
        $1 as raw_data,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @{{ var('opentarget_stage') }}/targets/*.parquet
)

SELECT
    id,
    approved_name,
    approved_symbol,
    biotype,
    genomic_location,
    protein_annotations,
    tractability,
    safety_liabilities,
    chemical_probes,
    hallmarks,
    synonyms,
    obsolete_names,
    obsolete_symbols,
    raw_data,
    loaded_at
FROM source_data
