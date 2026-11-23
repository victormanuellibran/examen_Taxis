{{ config(materialized='table') }}

select
    cast('{{ var('comparison_month') }}' as varchar) as comparison_month,
    cast('{{ var('target_month') }}' as varchar) as target_month
