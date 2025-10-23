-- models/staging/stg_tickets.sql

with source as (

    select * from {{ source('raw_data', 'raw_chamados_suporte') }}

),

renamed_and_deduped as (
    -- This CTE handles deduplication
    -- We partition by 'id_chamado' (ticket_id) and order by opening date
    -- to get the first instance (rank = 1)
    
    select
        *,
        row_number() over(
            partition by id_chamado 
            order by data_abertura asc
        ) as duplicate_rank
    from source
),

final as (

    select
        -- IDs
        id_chamado as ticket_id,
        id_cliente as customer_id,

        -- Timestamps
        data_abertura as opened_at,

        -- Dirty Column 1: 'tipo_chamado' (typos, spaces)
        case
            when lower(trim(tipo_chamado)) in ('bug crítico', 'bug critico', 'bug') then 'bug'
            when lower(trim(tipo_chamado)) in ('dúvida', ' duvida') then 'question'
            when lower(trim(tipo_chamado)) in ('solicitação feature', 'solicitação') then 'feature_request'
            else 'other'
        end as ticket_type,

        -- Dirty Column 2: 'prioridade' (nulls)
        coalesce(prioridade, 'Não Definida') as priority

    from renamed_and_deduped
    where 
        -- This filter removes all duplicates
        duplicate_rank = 1
)

select * from final