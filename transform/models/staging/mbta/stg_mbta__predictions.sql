with source as (
    select * from {{ source('mbta', 'predictions') }}
),

cleaned as (
    select
        -- identifiers / foreign keys
        id                                  as prediction_id,
        route_id,
        stop_id,
        trip_id,
        vehicle_id,

        -- event timestamps: raw are ISO-8601 strings with a tz offset
        try_to_timestamp_tz(arrival_time)   as arrival_at,
        try_to_timestamp_tz(departure_time) as departure_at,

        -- prediction attributes
        direction_id,
        stop_sequence,
        schedule_relationship,
        status,
        update_type,
        revenue,
        last_trip,
        arrival_uncertainty,
        departure_uncertainty,

        -- audit
        loaded_at
    from source
)

select *
from cleaned
qualify row_number() over (
    partition by prediction_id, loaded_at                       -- the composite grain
    order by coalesce(arrival_at, departure_at) desc nulls last -- deterministic tiebreaker for true dups
) = 1
