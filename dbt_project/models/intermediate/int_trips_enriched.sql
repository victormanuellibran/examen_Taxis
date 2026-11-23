{{ config(materialized='view') }}

-- La transformación Python ya produjo esta capa curada. dbt la consume como
-- vista para no recalcular ni materializar otra copia de los viajes.
select * from {{ ref('stg_yellow_trips') }}
