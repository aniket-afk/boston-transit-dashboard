with source as (
    select * from {{ source('mbta', 'routes') }}
),

cleaned as (
    select
        id                                  as route_id,
        long_name                           as route_long_name,
        short_name                          as route_short_name,
        fare_class,
        color                               as route_color,
        text_color,
        listed_route,
        sort_order,
        agency_id,
        line_id,

        -- decode MBTA route_type integer into a human label
        type                                as route_type_code,
        case type
            when 0 then 'Light Rail'
            when 1 then 'Heavy Rail'
            when 2 then 'Commuter Rail'
            when 3 then 'Bus'
            when 4 then 'Ferry'
            else 'Unknown'
        end                                 as route_type,

        -- unpack the VARIANT arrays: index 0/1 line up with direction_id 0/1
        direction_names[0]::string          as direction_0_name,
        direction_names[1]::string          as direction_1_name,
        direction_destinations[0]::string   as direction_0_destination,
        direction_destinations[1]::string   as direction_1_destination,

        loaded_at
    from source
)

select * from cleaned
