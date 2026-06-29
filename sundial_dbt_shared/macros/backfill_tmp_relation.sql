{# Suffix __dbt_tmp with chunk start date when backfill vars are set. #}

{% macro backfill_tmp_suffix(base_suffix='__dbt_tmp') %}
  {%- set chunk_start = var('backfill_start_ts', none) -%}
  {%- if chunk_start is not none and var('backfill_chunk_id', none) is not none -%}
    {%- set day = (chunk_start | string)[:10] | replace('-', '') -%}
    {{ return(base_suffix ~ '__' ~ day) }}
  {%- endif -%}
  {{ return(base_suffix) }}
{% endmacro %}


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
  {# Snowflake incremental drops tmp_relation before post_hooks; nothing left to drop. #}
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
