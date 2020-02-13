--------------------------------------------------------------------------------------------------------------
---- cloning a table relation
--------------------------------------------------------------------------------------------------------------	
{% macro clone_table_relation_if_exists(old_relation ,clone_relation) %}
  {% if old_relation is not none %}
    {{ log("Cloning existing relation " ~ old_relation ~ " as a backup with name " ~ clone_relation) }}
    {% call statement('clone_relation', auto_begin=False) -%}
        CREATE OR REPLACE TABLE {{ clone_relation }}
            CLONE {{ old_relation }}
    {%- endcall %}
  {% endif %}
{% endmacro %}

--------------------------------------------------------------------------------------------------------------
---- Backing up (Copy of) a (transient) table relation
--------------------------------------------------------------------------------------------------------------	
{% macro copyof_table_relation_if_exists(old_relation ,clone_relation) %}
  {% if old_relation is not none %}
    {{ log("Copying of existing relation " ~ old_relation ~ " as a backup with name " ~ clone_relation) }}
    {% call statement('clone_relation', auto_begin=False) -%}
        CREATE OR REPLACE TABLE {{ clone_relation }}
            AS SELECT * FROM {{ old_relation }}
    {%- endcall %}
  {% endif %}
{% endmacro %}

--------------------------------------------------------------------------------------------------------------
---- Create table 
--------------------------------------------------------------------------------------------------------------	
{%- macro create_table_stmt_fromfile(relation, sql) -%}
    {{ log("Creating table " ~ relation) }}

    {{ sql }}
    ;
{%- endmacro -%}

--------------------------------------------------------------------------------------------------------------
---- Create Procedure 
--------------------------------------------------------------------------------------------------------------	
{%- macro create_stmt_fromfile(sql) -%}
	{{ log("Creating procedure ") }}
    {{ sql }}
    ;
{%- endmacro -%}

{%- macro create_stage_stmt_fromfile(sql) -%}
    {{ log("Creating stage " ~ relation) }}
    {{ sql }}
{%- endmacro -%}

{% macro drop_orphans() %}

--------------------------------------------------------------------------------------------------------------
---- Getting all tables from database schema by using util function of dbt and setting them as relation and storing in separate list
--------------------------------------------------------------------------------------------------------------
    {%- set tbl_relations = [] -%}
    {%- set table_relations = [] -%}
	{%- set myprocedures = [] -%}
    {%- call statement('get_tables', fetch_result=True) %}
      {{ dbt_utils.get_tables_by_prefix_sql(schema, prefix, exclude, database) }}
    {%- endcall -%}
    {%- set table_list = load_result('get_tables') -%}

    {%- if table_list and table_list['table'] -%}
        {%- for row in table_list['table'] -%}
			{%- do tbl_relations.append(row.table_name) -%}
			{%- set source_relation = adapter.get_relation(
			 database=database,
			 schema=schema,
			 identifier=row.table_name) -%}
			  
			{% if source_relation is not none   %}
				{%- do table_relations.append(source_relation) -%}	
				{{ log("Source Relation: " ~ source_relation, info=true) }}
			{% endif %}		
        {%- endfor -%}
    {%- endif -%}

--------------------------------------------------------------------------------------------------------------
---- Saving models of dbt project in list 
--------------------------------------------------------------------------------------------------------------
  {% set models_in_project = graph.nodes.values() | selectattr('resource_type', 'sameas', 'model') | map(attribute='name') | list %}

	{%- call statement('get_tables1', fetch_result=True) %}
        select distinct 
            PROCEDURE_SCHEMA as "PROCEDURE_SCHEMA", PROCEDURE_NAME as "PROCEDURE_NAME" , ARGUMENT_SIGNATURE as "ARGUMENT_SIGNATURE"
        from {{database}}.information_schema.procedures
        where PROCEDURE_SCHEMA ilike '{{ schema }}'
	{%- endcall -%}

--------------------------------------------------------------------------------------------------------------
---- Saving procedures of database schema in list 
---- Concatinating Procedure signature to drop it. row.ARGUMENT_SIGNATURE also return variable name this is present in function parameter. But don't need it while dropping procedure.
---- E.g row.PROCEDURE_NAME return DROPPROCEDURE and row.ARGUMENT_SIGNATURE return (PROCEDURE_NAME VARCHAR) we need DROPPROCEDURE(VARCHAR) to drop it. 
--------------------------------------------------------------------------------------------------------------	
{%- set ProceduresList = load_result('get_tables1') -%}
  {{ log("Procedures in database " ~ ProceduresList ) }}
  {% for row in ProceduresList['table'] %}
    {{ log("procedure name " ~ row.PROCEDURE_NAME) }}
	{{ log("procedure signature " ~ row.ARGUMENT_SIGNATURE) }}
	
	{%- set val1 = row.ARGUMENT_SIGNATURE.split() -%}
	{% if val1|length > 1 -%}
		{%- set proc  = row.PROCEDURE_NAME + "(" + row.ARGUMENT_SIGNATURE.split()[1] -%}
	{% else -%}
		{%- set proc  = row.PROCEDURE_NAME + "()" -%}
	{% endif %}
	{%- do myprocedures.append(proc) -%}	
	
	{% if row.PROCEDURE_NAME not in models_in_project   %}
      {{ log(proc ~ " is orphan procedure. calling a statement to drop it.", info=True) }}
			{% call statement('drop_procedure') %}
				drop procedure if exists {{ proc }}
			   {{ log("dropping procedure " ~ "{{ proc }}" , info=true) }}
			{%- endcall %}
	{% endif %}	  
  {% endfor %}

--------------------------------------------------------------------------------------------------------------
---- Dropping Table
--------------------------------------------------------------------------------------------------------------	
  {% for table in table_relations %}
    {% if table['table'] not in models_in_project   %}
      {{ log(table['table'] ~ " is orphan. calling a statement to drop it.", info=True) }}
	  {{ adapter.drop_relation(table) }}
	{% endif %}	
  {% endfor %}

{% endmacro %}

{% macro get_tables_by_prefix_sql(schema, prefix, exclude='', database=target.database) %}
    {{ adapter_macro('dbt_utils.get_tables_by_prefix_sql', schema, prefix, exclude, database) }}
{% endmacro %}

{% macro default__get_tables_by_prefix_sql(schema=target.schema, prefix= target.prefix, exclude='', database=target.database) %}

        select distinct 
            table_schema as "table_schema", table_name as "table_name"
        from {{database}}.information_schema.tables
        where table_schema ilike '{{ schema }}'

{% endmacro %}

{% macro bigquery__get_tables_by_prefix_sql(schema, prefix, exclude='', database=target.database) %}
    
        select distinct
            dataset_id as table_schema, table_id as table_name

        from {{adapter.quote(database)}}.{{schema}}.__TABLES_SUMMARY__
        where dataset_id = '{{schema}}'
            and lower(table_id) like lower ('{{prefix}}%')
            and lower(table_id) not like lower ('{{exclude}}')

{% endmacro %}