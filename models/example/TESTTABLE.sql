{{
    config(materialized='persistent_table'
        ,retain_previous_version_flg=false
        ,migrate_data_over_flg=false
    )
}}

CREATE OR REPLACE TABLE "{{ database }}"."{{ schema }}"."TESTTABLE" (
             col1 varchar,
	     col2 number(38,0),
	     col3 varchar2
)
