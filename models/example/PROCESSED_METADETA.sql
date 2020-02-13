{{
    config(materialized='persistent_table'
        ,retain_previous_version_flg=false
        ,migrate_data_over_flg=false
    )
}}

CREATE OR REPLACE TABLE "{{ database }}"."{{ schema }}"."PROCESSED_METADETA" (
             MappingId         STRING,
             UltimateId        BIGINT,
             CountryIso        STRING,
             ProductId         INT, 
             FeatureString     STRING,
             VersionNumber     BIGINT,
             MachineName       STRING,
             MachineSid        STRING,
             UserName          STRING,
             UserSid           STRING,
             FeatureId         STRING,
             ImsId             STRING, 
             ProjectId         STRING,
             SessionId         STRING,
                   Key         STRING,
                 Value         STRING,
              MetaData         STRING, 
             UsageDate           DATE,
            UsageMonth             INT

)
