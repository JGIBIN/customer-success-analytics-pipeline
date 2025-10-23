-- models/staging/stg_churn.sql
-- This is our target variable. It's already "clean".
-- We just rename columns for consistency.

with source as (

    select * from {{ source('raw_data', 'raw_churn_target') }}

),

renamed as (

    select
        id_cliente as customer_id,
        churn_status
    from source

)

select * from renamed