-- models/staging/stg_customers.sql

with source as (

    -- This is a dbt 'source'.
    -- It points to the cs_ops_raw_data.raw_clientes table
    -- (We will configure this in the next step!)
    select * from {{ source('raw_data', 'raw_clientes') }}

),

renamed as (

    select
        -- Clean columns (just rename)
        id_cliente as customer_id,
        nome_empresa as company_name,
        data_contrato as contract_date,

        -- Dirty Column 1: 'plano' (with nulls, typos, spaces)
        case 
            when lower(trim(plano)) in ('basic', 'básico') then 'basic'
            when lower(trim(plano)) in ('pro', 'proo') then 'pro'
            when lower(trim(plano)) = 'enterprise' then 'enterprise'
            else 'undefined' -- Catches Nulls and anything else
        end as plan,

        -- Dirty Column 2: 'mrr_raw' (string with "R$ 1.234,50" and "Inválido")
        safe_cast(
            replace(
                replace(mrr_raw, 'R$ ', ''), 
                ',', '.')
        as float64) as mrr

    from source
    where
        -- Filter out records that are impossible to clean
        mrr_raw != 'Inválido'

)

select * from renamed