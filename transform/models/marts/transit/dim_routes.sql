with routes as (
    select * from {{ ref('stg_mbta__routes') }}
)

select
    route_id,
    route_long_name,
    route_short_name,
    route_type,
    route_type_code,
    fare_class,
    route_color,
    text_color,
    direction_0_name,
    direction_1_name,
    direction_0_destination,
    direction_1_destination,
    line_id,
    sort_order
from routes
