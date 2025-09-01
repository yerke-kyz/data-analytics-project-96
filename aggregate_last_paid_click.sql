with paid_visits as (
    select
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        row_number()
            over (partition by s.visitor_id order by s.visit_date desc)
            as rnk
    from sessions as s
    where s.medium in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),
last_paid_click as (
    select
        pv.visitor_id,
        pv.visit_date::date as visit_date,
        pv.source as utm_source,
        pv.medium as utm_medium,
        pv.campaign as utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    from paid_visits as pv
    left join leads as l
        on pv.visitor_id = l.visitor_id
    where rnk = 1
),
leads_math as (
    select
        l.visit_date,
        count(l.visitor_id) as visitors_count,
        l.utm_source,
        l.utm_medium,
        l.utm_campaign,
        count(l.lead_id) as leads_count,
        count(*) filter (
            where l.closing_reason = 'Успешно реализовано' or l.status_id = 142
        ) as purchaces_count,
        sum(l.amount) filter (
            where l.closing_reason = 'Успешно реализовано' or l.status_id = 142
        ) as revenue
    from last_paid_click as l
    group by 1, 3, 4, 5
)
select
    l.visit_date,
    l.visitors_count,
    l.utm_source,
    l.utm_medium,
    l.utm_campaign,
    coalesce(sum(vk.daily_spent),0) + coalesce(sum(ya.daily_spent),0) as total_cost,
    l.leads_count,
    l.purchaces_count,
    l.revenue
from leads_math as l
left join vk_ads as vk
    on
        l.utm_source = vk.utm_source
        and l.utm_medium = vk.utm_medium
        and l.utm_campaign = vk.utm_campaign
        and l.visit_date = vk.campaign_date::date
left join ya_ads as ya
    on
        l.utm_source = ya.utm_source
        and l.utm_medium = ya.utm_medium
        and l.utm_campaign = ya.utm_campaign
        and l.visit_date = ya.campaign_date::date
group by
    l.visit_date,
    l.visitors_count,
    l.utm_source,
    l.utm_medium,
    l.utm_campaign,
    l.leads_count,
    l.purchaces_count,
    l.revenue
order by
    visit_date asc,
    visitors_count desc,
    utm_source asc,
    utm_medium asc,
    utm_campaign asc,
    revenue desc nulls last
limit 15;
