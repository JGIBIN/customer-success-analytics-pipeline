-- models/staging/stg_usage.sql

with source as (

    select * from {{ source('raw_data', 'raw_uso_produto') }}

),

renamed as (

    select
        id_log as usage_log_id,
        id_cliente as customer_id,
        data_log as log_date,
        feature_usada as feature_used,
        minutos_na_feature as minutes_on_feature
    from source
    where
        -- Dirty Data P4: Remove 'orphan' records (logs from non-existent customers)
        -- We only keep logs from valid customers (starting with 'cli_')
        id_cliente like 'cli_%'

)

select * from renamed