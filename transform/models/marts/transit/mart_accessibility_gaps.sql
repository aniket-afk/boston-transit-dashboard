with predictions as (
    select * from {{ ref('fct_predictions') }}
    where delay_seconds is not null
    qualify row_number() over (
        partition by prediction_id
        order by loaded_at desc
    ) = 1
),

stops as (
    select * from {{ ref('dim_stops') }}
),

-- stop-level: does each stop have accessibility info, and how's its service?
stop_service as (
    select
        s.wheelchair_accessibility,
        s.stop_id,
        s.stop_name,
        s.municipality,
        p.delay_seconds
    from predictions p
    join stops s on p.stop_id = s.stop_id
),

by_accessibility as (
    select
        wheelchair_accessibility,

        count(distinct stop_id)                               as n_stops,
        count(*)                                              as n_predictions,
        round(avg(delay_seconds), 1)                          as avg_delay_seconds,
        round(avg(delay_seconds) / 60.0, 2)                   as avg_delay_minutes,
        round(median(delay_seconds), 1)                       as median_delay_seconds,

        round(100.0 * count_if(delay_seconds <= 60) / count(*), 1) as pct_on_time,

        {{ confidence_flag('count(*)') }}                     as confidence
    from stop_service
    group by 1
)

select * from by_accessibility
order by
    case wheelchair_accessibility
        when 'Accessible'     then 1
        when 'Not Accessible' then 2
        else 3
    end