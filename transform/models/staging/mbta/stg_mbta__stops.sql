with source as (
    select * from {{ source('mbta', 'stops') }}
),

cleaned as (
    select
        id                          as stop_id,
        name                        as stop_name,
        municipality,
        latitude,
        longitude,

        -- decode accessibility (GTFS wheelchair_boarding)
        wheelchair_boarding         as wheelchair_boarding_code,
        case wheelchair_boarding
            when 1 then 'Accessible'
            when 2 then 'Not Accessible'
            else 'No Information'
        end                         as wheelchair_accessibility,

        -- decode location_type (GTFS)
        location_type               as location_type_code,
        case location_type
            when 0 then 'Stop/Platform'
            when 1 then 'Station'
            when 2 then 'Entrance/Exit'
            when 3 then 'Generic Node'
            when 4 then 'Boarding Area'
            else 'Unknown'
        end                         as location_type,

        zone_id,
        parent_station_id,
        address,
        on_street,
        at_street,

        loaded_at
    from source
)

select * from cleaned
