-- models/marts/fct_customer_kpis.sql
-- VERSION 3: Even More Feature Engineering!

-- 1. Base Staging Tables (Sem mudanças aqui)
with customers as (
    select * from {{ ref('stg_customers') }}
),

tickets as (
    select * from {{ ref('stg_tickets') }}
),

usage as (
    select * from {{ ref('stg_usage') }}
),

churn as (
    select * from {{ ref('stg_churn') }}
),

-- 2. Feature Engineering CTEs (Aqui entram as novas lógicas)

customer_ticket_features as (
    select
        customer_id,
        count(ticket_id) as total_tickets,
        
        count(case 
            when ticket_type = 'bug' and priority = 'Alta' then ticket_id 
            else null 
        end) as total_critical_bug_tickets,
        
        count(case 
            when opened_at >= date_sub(current_date(), interval 30 day) then ticket_id 
            else null 
        end) as total_tickets_last_30d,

        max(opened_at) as last_ticket_date, -- << NOVA FEATURE >> (Para Recência)

        -- << NOVA FEATURE DE RATIO >>:
        safe_divide( 
            count(case when ticket_type = 'bug' and priority = 'Alta' then ticket_id else null end),
            count(ticket_id)
        ) as critical_ticket_ratio 
            
    from tickets
    group by 1
),

customer_usage_features as (
    select
        customer_id,
        count(usage_log_id) as total_logins,
        sum(minutes_on_feature) as total_minutes_on_platform,
        avg(minutes_on_feature) as avg_minutes_per_session,        
        count(distinct feature_used) as distinct_features_used,
        max(log_date) as last_login_date,

        -- << NOVA FEATURE FLAG >>:
        max(case 
            when feature_used like '%financeiro%' or feature_used like '%nfe%' then 1 
            else 0 
        end) as used_financial_features_flag 
        
    from usage
    group by 1
),

-- 3. Final Join Logic (Aqui juntamos tudo e calculamos features combinadas)
final as (
    select
        -- Keys
        c.customer_id,
        
        -- Customer details
        c.company_name,
        c.plan,
        c.mrr,
        c.contract_date,

        -- Calculated Features
        date_diff(current_date(), date(c.contract_date), day) as customer_tenure_days,

        coalesce(ctf.total_tickets, 0) as total_tickets,
        coalesce(ctf.total_critical_bug_tickets, 0) as total_critical_tickets, 
        coalesce(ctf.total_tickets_last_30d, 0) as tickets_last_30d, -- << NOVA FEATURE >> -- CORRIGIDO
        coalesce(ctf.critical_ticket_ratio, 0) as critical_ticket_ratio, -- << NOVA FEATURE >>

        coalesce(cuf.total_logins, 0) as total_logins,
        coalesce(cuf.total_minutes_on_platform, 0) as total_minutes,
        coalesce(cuf.avg_minutes_per_session, 0) as avg_session_minutes,
        coalesce(cuf.distinct_features_used, 0) as distinct_features_used, 
        coalesce(cuf.used_financial_features_flag, 0) as used_financial_features_flag, -- << NOVA FEATURE >>

        coalesce(
            date_diff(current_date(), date(cuf.last_login_date), day), 
            9999 
        ) as days_since_last_login, 

        -- << NOVA FEATURE >> (Recência de Tickets):
        coalesce(
            date_diff(current_date(), date(ctf.last_ticket_date), day),
            9999 
        ) as days_since_last_ticket,

        -- << NOVA FEATURE >> (Recência Combinada):
        least( 
             coalesce(date_diff(current_date(), date(cuf.last_login_date), day), 9999),
             coalesce(date_diff(current_date(), date(ctf.last_ticket_date), day), 9999)
        ) as days_since_last_interaction,

        -- << NOVA FEATURE >> (Ratio Combinado):
        safe_divide(
            coalesce(ctf.total_tickets, 0),
            coalesce(cuf.total_logins, 0) 
        ) as tickets_per_login,
        
        -- Target variable
        ch.churn_status

    from customers c
    left join customer_ticket_features ctf on c.customer_id = ctf.customer_id
    left join customer_usage_features cuf on c.customer_id = cuf.customer_id
    left join churn ch on c.customer_id = ch.customer_id
)

select * from final