{{
config(materialized='procedure_definition'
)
}}

CREATE or replace PROCEDURE "{{ database }}"."{{ schema }}"."DROPPROCEDURE" (PROCEDURE_NAME string)
RETURNS string
LANGUAGE javascript
AS
$$
    var sql_command = " drop procedure " +  PROCEDURE_NAME ;
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Procedure dropped.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed to drop procedure: " + err;   // Return a success/error indicator.
        }
$$