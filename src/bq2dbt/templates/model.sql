{{
    config(
        materialized='view'
    )
}}

-- Original BigQuery view: {{ source_view }}
-- Generated by bq2dbt at {{ timestamp }}

{{ sql_definition }} 