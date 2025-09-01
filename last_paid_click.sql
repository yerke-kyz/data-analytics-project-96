/*
visitor_id — уникальный человек на сайте
visit_date — время визита
utm_source / utm_medium / utm_campaign — метки c учетом модели атрибуции
lead_id — идентификатор лида, если пользователь сконвертился в лид после(во время) визита, NULL — если пользователь не оставил лид
created_at — время создания лида, NULL — если пользователь не оставил лид
amount — сумма лида (в деньгах), NULL — если пользователь не оставил лид
closing_reason — причина закрытия, NULL — если пользователь не оставил лид
status_id — код причины закрытия, NULL — если пользователь не оставил ли
s.source filter (where s.source in ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'))
 */

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
)
select
    pv.visitor_id,
    pv.visit_date,
    pv.source,
    pv.medium,
    pv.campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
from paid_visits as pv
left join leads as l
    on pv.visitor_id = l.visitor_id
where rnk = 1
order by
l.amount desc nulls last, pv.visit_date asc, pv.source asc, pv.medium asc, pv.campaign asc;
