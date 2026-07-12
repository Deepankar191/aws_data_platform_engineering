-- =============================================================================
-- credit_silver.aml_screening  (SPEC §3 silver, source #3 conformed)
-- -----------------------------------------------------------------------------
-- Cleaned, typed, deduped AML / PEP screening results. Delta table read by Athena v3.
-- One row per aml_case_id (bronze screening_ref; latest screening per name+dob). Match
-- is fuzzy by construction (soundex(full_name)+dob, SPEC §6.4) -> always via probabilistic scorer.
-- Carries silver audit block. Timestamps GST (UTC+4).
-- Schema matches glue/silver_layer/aml_to_silver.py (the Delta writer / catalog owner).
-- =============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS credit_silver.aml_screening (
    aml_case_id          STRING     COMMENT 'AML screening reference. Natural key / grain of this table (bronze screening_ref)',
    full_name            STRING     COMMENT 'PII Level 2: screened subject full name',
    date_of_birth        DATE       COMMENT 'PII Level 2: subject date of birth — match key with name soundex',
    name_soundex         STRING     COMMENT 'PII Level 3: soundex code of full_name — fuzzy match key (SPEC §6.4)',
    aml_status           STRING     COMMENT 'Screening outcome. Values: CLEAR, HIT, PENDING',
    is_pep               BOOLEAN    COMMENT 'True if subject flagged as a Politically Exposed Person',
    screening_timestamp  TIMESTAMP  COMMENT 'Provider screening time, GST',
    source_system        STRING     COMMENT 'Audit: originating system. Constant "AML"',
    batch_id             STRING     COMMENT 'Audit: Glue run/batch id that produced this silver row',
    created_timestamp    TIMESTAMP  COMMENT 'Audit: when this silver row was first written, GST',
    updated_timestamp    TIMESTAMP  COMMENT 'Audit: when this silver row was last updated, GST'
)
LOCATION 's3://wio-credit-decision-${ENV}/silver/aml_screening/'
TBLPROPERTIES (
    'table_type' = 'DELTA'
);
