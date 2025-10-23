-- models/marts/fct_customer_kpis.sql

-- 1. Get our clean base tables
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

-- 2. Calculate aggregations (KPIs)
customer_tickets as (
    -- Calculate total tickets per customer
    select
        customer_id,
        count(ticket_id) as total_tickets
    from tickets
    group by 1
),

customer_usage as (
    -- Calculate total usage minutes and avg minutes per session
    select
        customer_id,
        sum(minutes_on_feature) as total_minutes_on_platform,
        avg(minutes_on_feature) as avg_minutes_per_session,
        count(usage_log_id) as total_logins
    from usage
    group by 1
),

-- 3. Join everything together into our final table
final as (
    select
        -- Keys
        c.customer_id,
        
        -- Customer details
        c.company_name,
        c.plan,
        c.mrr,
        c.contract_date,

        -- Ticket KPIs
        coalesce(ct.total_tickets, 0) as total_tickets,

        -- Usage KPIs
        coalesce(cu.total_logins, 0) as total_logins,
        coalesce(cu.total_minutes_on_platform, 0) as total_minutes,
        coalesce(cu.avg_minutes_per_session, 0) as avg_session_minutes,
        
        -- Target variable
        ch.churn_status

    from customers c

    -- Use left joins to ensure we keep all customers, 
    -- even if they have no tickets or usage
    left join customer_tickets ct on c.customer_id = ct.customer_id
    left join customer_usage cu on c.customer_id = cu.customer_id
    left join churn ch on c.customer_id = ch.customer_id
)

select * from final