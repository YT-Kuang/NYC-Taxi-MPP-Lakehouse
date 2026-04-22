select
    pickup_borough,
    pickup_zone,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_revenue_per_trip,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_min) as avg_trip_duration_min,
    avg(trip_speed_mph) as avg_trip_speed_mph,
    avg(fare_per_mile) as avg_fare_per_mile
from {{ ref('fact_trip') }}
group by 1, 2