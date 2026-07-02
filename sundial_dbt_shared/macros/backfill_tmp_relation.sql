{# Suffix __dbt_tmp with the chunk start date when backfill vars are set, so
   parallel chunks of one model each build their own staging table instead of
   racing on a single `<model>__dbt_tmp`. Requires the tenant `dispatch:` config
   below so dbt's built-in make_temp_relation routes here. #}

{% macro backfill_tmp_suffix(base_suffix='__dbt_tmp') %}
  {%- set chunk_start = var('backfill_start_ts', none) -%}
  {%- if chunk_start is not none and var('backfill_chunk_id', none) is not none -%}
    {%- set day = (chunk_start | string)[:10] | replace('-', '') -%}
    {{ return(base_suffix ~ '__' ~ day) }}
  {%- endif -%}
  {{ return(base_suffix) }}
{% endmacro %}


{# Dispatched make_temp_relation. Tenant dbt_project.yml must route the dbt
   namespace here:
     dispatch:
       - macro_namespace: dbt
         search_order: ['<your_dbt_project>', 'sundial_dbt_shared', 'dbt']
   so dbt's incremental materialization picks up the per-chunk suffix. #}
{% macro default__make_temp_relation(base_relation, suffix='__dbt_tmp') %}
  {%- set suffix = sundial_dbt_shared.backfill_tmp_suffix(suffix) -%}
  {%- set temp_identifier = base_relation.identifier ~ suffix -%}
  {%- set temp_relation = base_relation.incorporate(
        path={"identifier": temp_identifier}) -%}
  {{ return(temp_relation) }}
{% endmacro %}


{% macro make_temp_relation(base_relation, suffix='__dbt_tmp') %}
  {{ return(sundial_dbt_shared.default__make_temp_relation(base_relation, suffix)) }}
{% endmacro %}


{% macro drop_backfill_tmp_table() %}
  {%- if not execute -%}
    {{ return('') }}
  {%- endif -%}
  {%- if var('backfill_chunk_id', none) is none -%}
    {{ return('') }}
  {%- endif -%}
  {%- if this is none -%}
    {{ return('') }}
  {%- endif -%}
  {# Snowflake's incremental materialization drops its tmp_relation before
     post-hooks run, so the suffixed staging table is already gone here. #}
  {%- if target.type == 'snowflake' -%}
    {{ return('') }}
  {%- endif -%}
  {%- set tmp = sundial_dbt_shared.default__make_temp_relation(this).incorporate(type='table') -%}
  {%- set existing = adapter.get_relation(tmp.database, tmp.schema, tmp.identifier) -%}
  {%- if existing is none -%}
    {{ return('') }}
  {%- endif -%}
  {{ log('Dropping backfill temp table ' ~ existing, info=true) }}
  {%- do adapter.drop_relation(existing) -%}
{% endmacro %}
