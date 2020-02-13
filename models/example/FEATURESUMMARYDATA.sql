{{
    config(materialized='persistent_table'
        ,retain_previous_version_flg=false
        ,migrate_data_over_flg=false
    )
}}

CREATE OR REPLACE TABLE "{{ database }}"."{{ schema }}"."FEATURESUMMARYDATA" (
UltimateId BIGINT,
CountryIso STRING, 
MachineSid STRING, 
ProductId INT,
FeatureString STRING,
VersionNumber BIGINT,
ProjectId STRING, 
ImsId STRING, 
UsageDate DATE,
FeatureId STRING, 
Source STRING,
UsageMonth INT,
Count INT,
Duration INT
)
