with predictions as (
    select * from {{ ref('fct_predictions') }}
    where delay_seconds is not null
    qualify row_number() over (
        partition by prediction_id
        order by loaded_at desc
    ) = 1
),

joined as (
    select
        s.municipality,
        p.delay_seconds,
        p.route_id
    from predictions p
    join {{ ref('dim_stops') }} s on p.stop_id = s.stop_id
    where s.municipality is not null
),

by_neighborhood as (
    select
        municipality,

        count(*)                                              as n_predictions,
        count(distinct route_id)                              as n_routes,
        round(avg(delay_seconds), 1)                          as avg_delay_seconds,
        round(avg(delay_seconds) / 60.0, 2)                   as avg_delay_minutes,
        round(median(delay_seconds), 1)                       as median_delay_seconds,
        max(delay_seconds)                                    as max_delay_seconds,

        round(100.0 * count_if(delay_seconds <= 60) / count(*), 1) as pct_on_time,
        round(100.0 * count_if(delay_seconds  > 60) / count(*), 1) as pct_late,
        -- guard against reading single-observation "averages" as signal
        {{ confidence_flag('count(*)') }} as confidence
    from joined
    group by 1
)

select * from by_neighborhood
order by avg_delay_seconds desc