select *
from {{ ref('fact_trip') }}
where total_amount < 0
   or fare_amount < 0
   or tip_amount < 0