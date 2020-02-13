{{
    config(materialized='persistent_table'
        ,retain_previous_version_flg=false
        ,migrate_data_over_flg=false
    )
}}

CREATE OR REPLACE TABLE "{{ database }}"."{{ schema }}"."CLIENTDETAILSBYMACHINE" (
UltimateId BIGINT,
CountryIso STRING,
ProductId INT,
FeatureString STRING,
VersionNumber BIGINT,
MachineSid STRING,
LicClientVersion STRING,
OSVersion STRING,
LicClientArch STRING,
OSArchitecture STRING,
UsageWeek INT,
UsageMonth INT
)
