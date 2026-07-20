with predictions as (
    select * from {{ ref('fct_predictions') }}
    qualify row_number() over (
        partition by prediction_id
        order by loaded_at desc
    ) = 1
),

enriched as (
    select
        -- time buckets derived from the predicted arrival
        date_trunc('hour', predicted_arrival_at)        as service_hour,
        date_trunc('day',  predicted_arrival_at)::date  as service_date,
        hour(predicted_arrival_at)                       as hour_of_day,

        route_id,
        delay_seconds,
        schedule_relationship
    from predictions
    where predicted_arrival_at is not null
),

by_hour as (
    select
        service_date,
        service_hour,
        hour_of_day,

        count(*)                                             as n_predictions,
        count(distinct route_id)                             as n_routes_active,

-- service disposition (only SKIPPED and blank/normal appear in this feed)
        count_if(schedule_relationship is null
                 or schedule_relationship = '')             as n_normal,
        count_if(schedule_relationship = 'SKIPPED')          as n_skipped,

        -- delay metrics only where we have a schedule to compare against
        round(avg(delay_seconds), 1)                         as avg_delay_seconds,
        count_if(delay_seconds is not null)                  as n_with_delay,

        {{ confidence_flag('count(*)') }}                    as confidence
    from enriched
    group by 1, 2, 3
)

select * from by_hour
order by service_hour