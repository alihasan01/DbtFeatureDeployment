{{
    config(materialized='persistent_table'
        ,retain_previous_version_flg=false
        ,migrate_data_over_flg=false
    )
}}

CREATE OR REPLACE TABLE "{{ database }}"."{{ schema }}"."PROCESSED_FEATURELOGS" (
             MappingId     STRING,
             UltimateId     BIGINT,  
             CountryIso     STRING,
              ProductId     INT,
          FeatureString     STRING,
          VersionNumber     BIGINT,
            MachineName     STRING,
             MachineSid     STRING,
               UserName     STRING,
                UserSid     STRING, 
                  ImsId     STRING, 
              ProjectId     STRING,
              SessionId     STRING,
              FeatureId     STRING, 
              StartTime     TIMESTAMP,
                EndTime     TIMESTAMP,
                  Count     INT,
               Duration     INT,
        DurationTracked     BOOLEAN,
                 Source     STRING,
              UsageDate     DATE,
             UsageMonth     INT

)
