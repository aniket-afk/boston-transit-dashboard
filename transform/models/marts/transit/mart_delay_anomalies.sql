with predictions as (
    select * from {{ ref('fct_predictions') }}
    where delay_seconds is not null
    qualify row_number() over (
        partition by prediction_id
        order by loaded_at desc
    ) = 1
),

-- compute mean & stddev across the whole population, attached to every row
stats as (
    select
        p.*,
        avg(delay_seconds)    over ()  as pop_mean_delay,
        stddev(delay_seconds) over ()  as pop_stddev_delay
    from predictions p
),

scored as (
    select
        prediction_id,
        route_id,
        stop_id,
        predicted_arrival_at,
        scheduled_arrival_at,
        delay_seconds,
        round(pop_mean_delay, 1)                                      as pop_mean_delay,
        round(pop_stddev_delay, 1)                                   as pop_stddev_delay,

        -- z-score: how many stddevs from the mean (guard against divide-by-zero)
        case
            when pop_stddev_delay = 0 or pop_stddev_delay is null then 0
            else round((delay_seconds - pop_mean_delay) / pop_stddev_delay, 2)
        end                                                          as delay_zscore
    from stats
),

flagged as (
    select
        *,
        case
            when abs(delay_zscore) >= 2 then true
            else false
        end                                                          as is_anomaly,
        case
            when delay_zscore >=  2 then 'unusually_late'
            when delay_zscore <= -2 then 'unusually_early'
            else 'normal'
        end                                                          as anomaly_type
    from scored
)

select *
from flagged
where is_anomaly = true      -- surface only the outliers
order by abs(delay_zscore) desc