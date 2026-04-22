select *
from {{ ref('mart_hourly_demand') }}
where trip_count < 0
   or total_revenue < 0
   or avg_total_amount < 0