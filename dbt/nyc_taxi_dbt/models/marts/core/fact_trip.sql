select
    row_number() over (
        order by pickup_ts, dropoff_ts, pu_location_id, do_location_id
    ) as trip_sk,

    pickup_ts,
    dropoff_ts,
    pickup_date,
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_hour,
    pickup_weekday,

    vendor_id,
    pu_location_id,
    do_location_id,
    passenger_count,

    trip_distance,
    trip_duration_min,
    trip_duration_hour,
    trip_speed_mph,

    fare_amount,
    extra_amount,
    mta_tax_amount,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    fare_per_mile,

    payment_type,
    rate_code,
    store_and_forward_flag,
    is_airport_trip,

    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,
    dropoff_service_zone

from {{ ref('stg_trip_enriched') }}