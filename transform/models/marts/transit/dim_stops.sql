with stops as (
    select * from {{ ref('stg_mbta__stops') }}
)

select
    stop_id,
    stop_name,
    municipality,
    latitude,
    longitude,
    wheelchair_accessibility,
    wheelchair_boarding_code,
    location_type,
    location_type_code,
    zone_id,
    parent_station_id
from stops
