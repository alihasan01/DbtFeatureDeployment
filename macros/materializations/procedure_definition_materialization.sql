{%- materialization procedure_definition, adapter='snowflake' -%}

{%- set identifier = model['alias'] -%}
{%- set current_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
{%- set target_relation = api.Relation.create( identifier=identifier, schema=schema, database=database) -%}

{%- set has_transactional_hooks = (hooks | selectattr('transaction', 'equalto', True) | list | length) > 0 %}
{% set existing_relation = load_relation(this) %}

 {%- set backup_suffix_dt = py_current_timestring() -%}
 {%- set backup_table_suffix = config.get('backup_table_suffix', default='_DBT_BACKUP_') -%}
 {%- set backup_identifier = model['name'] + backup_table_suffix + backup_suffix_dt -%}
 {%- set backup_relation = api.Relation.create(database=database,
                                               schema=schema,
                                               identifier=backup_identifier) -%}


	{%- set current_relation_exists_as_table = (current_relation is not none and current_relation.is_procedure) -%}


{{ run_hooks(pre_hooks, inside_transaction=False) }}
-- BEGIN happens here:
{{ run_hooks(pre_hooks, inside_transaction=True) }}

-- build model

{% call statement('main') -%}
  {{ create_stmt_fromfile(sql) }}
{%- endcall %}

     -- backup the existing table
    {% if current_relation_exists_as_table %}
        {{ clone_table_relation_if_exists(current_relation ,backup_relation) }}
    {% endif %}
	
{{ log("target_relation: " ~ target_relation , info=true) }}
{{ log("current_relation: " ~ current_relation , info=true) }}

{{ drop_orphans() }}
{{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}
{{ run_hooks(post_hooks, inside_transaction=False) }}
{{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}