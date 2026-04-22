select
    pickup_date,
    count(*) as trip_count,
    sum(passenger_count) as passenger_count_total,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_ticket_size,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_min) as avg_trip_duration_min,
    avg(trip_speed_mph) as avg_trip_speed_mph,
    sum(case when is_airport_trip then 1 else 0 end) as airport_trip_count
from {{ ref('fact_trip') }}
group by 1