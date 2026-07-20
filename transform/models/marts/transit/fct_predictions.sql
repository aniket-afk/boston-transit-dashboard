with predictions as (
    select * from {{ ref('stg_mbta__predictions') }}
),

schedules as (
    select * from {{ ref('stg_mbta__schedules') }}
),

joined as (
    select
        -- degenerate key: the grain of the fact
        p.prediction_id,
        p.loaded_at,

        -- foreign keys to dimensions
        p.route_id,
        p.stop_id,
        p.trip_id,
        p.vehicle_id,
        p.direction_id,

        -- predicted ("actual expected") times
        p.arrival_at            as predicted_arrival_at,
        p.departure_at          as predicted_departure_at,

        -- scheduled ("supposed to") times, from the timetable join
        s.scheduled_arrival_at,
        s.scheduled_departure_at,

        -- THE measure: positive = late, negative = early, null = no schedule (ADDED trips)
        datediff('second', s.scheduled_arrival_at, p.arrival_at) as delay_seconds,

        -- prediction context
        p.schedule_relationship,
        p.status,
        p.update_type,
        p.stop_sequence,
        p.arrival_uncertainty,
        p.departure_uncertainty
    from predictions p
    left join schedules s
        on  p.trip_id = s.trip_id
        and p.stop_id = s.stop_id
)

select * from joined
