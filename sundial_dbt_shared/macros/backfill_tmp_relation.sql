{# Parallel chunked backfill: unique __dbt_tmp per chunk (backfill_chunk_id var). #}

{% macro backfill_tmp_suffix(base_suffix='__dbt_tmp') %}
  {%- set chunk_id = var('backfill_chunk_id', none) -%}
  {%- if chunk_id is not none -%}
    {{ return(base_suffix ~ '__' ~ (chunk_id | replace('-', ''))) }}
  {%- endif -%}
  {{ return(base_suffix) }}
{% endmacro %}


{% macro make_temp_relation(base_relation, suffix='__dbt_tmp') %}
  {%- set suffix = sundial_dbt_shared.backfill_tmp_suffix(suffix) -%}
  {{ return(adapter.dispatch('make_temp_relation', 'dbt')(base_relation, suffix)) }}
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
  {%- set tmp = sundial_dbt_shared.make_temp_relation(this) -%}
  {{ log('Dropping backfill temp table ' ~ tmp, info=true) }}
  {%- do adapter.drop_relation(tmp) -%}
{% endmacro %}
