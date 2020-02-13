{{
config(materialized='procedure_definition'
)
}}

CREATE or replace PROCEDURE "{{ database }}"."{{ schema }}"."DROPTABLE" (TABLE_NAME string)
RETURNS string
LANGUAGE javascript
AS
$$
    var sql_command = " drop table " +  TABLE_NAME ;
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Table removed.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed to remove table: " + err;   // Return a success/error indicator.
        }
$$