with predictions as (
    select * from {{ ref('fct_predictions') }}
    where delay_seconds is not null          -- exclude ADDED/unscheduled trips
    qualify row_number() over (
        partition by prediction_id            -- collapse snapshots: one row per prediction
        order by loaded_at desc               -- keep the latest observation
    ) = 1
),

by_route as (
    select
        p.route_id,
        r.route_long_name,
        r.route_type,

        count(*)                                              as n_predictions,
        round(avg(p.delay_seconds), 1)                        as avg_delay_seconds,
        round(avg(p.delay_seconds) / 60.0, 2)                 as avg_delay_minutes,
        round(median(p.delay_seconds), 1)                     as median_delay_seconds,
        max(p.delay_seconds)                                  as max_delay_seconds,

        -- on-time = within 60s of schedule (transit industry convention varies; 60s is defensible)
        round(100.0 * count_if(p.delay_seconds <= 60) / count(*), 1) as pct_on_time,
        round(100.0 * count_if(p.delay_seconds  > 60) / count(*), 1) as pct_late
    from predictions p
    join {{ ref('dim_routes') }} r on p.route_id = r.route_id
    group by 1, 2, 3
)

select * from by_route
order by avg_delay_seconds desc