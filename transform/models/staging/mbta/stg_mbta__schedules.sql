with source as (
    select * from {{ source('mbta', 'schedules') }}
),

cleaned as (
    select
        id                                  as schedule_id,

        -- join keys back to predictions
        trip_id,
        stop_id,
        route_id,
        direction_id,
        stop_sequence,

        -- the "supposed to" times: ISO-8601 strings -> typed timestamps
        try_to_timestamp_tz(arrival_time)   as scheduled_arrival_at,
        try_to_timestamp_tz(departure_time) as scheduled_departure_at,

        -- schedule attributes
        timepoint,
        pickup_type,
        drop_off_type,

        loaded_at
    from source
)

select *
from cleaned
qualify row_number() over (
    partition by trip_id, stop_id
    order by scheduled_arrival_at desc nulls last
) = 1
