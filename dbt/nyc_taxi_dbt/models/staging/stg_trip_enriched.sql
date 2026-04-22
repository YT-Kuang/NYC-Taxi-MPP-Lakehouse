select
    -- natural-ish trip identifiers / timestamps
    cast(tpep_pickup_datetime as timestamp(6) with time zone) as pickup_ts,
    cast(tpep_dropoff_datetime as timestamp(6) with time zone) as dropoff_ts,
    cast(pickup_date as date) as pickup_date,
    cast(pickup_year as integer) as pickup_year,
    cast(pickup_month as integer) as pickup_month,
    cast(pickup_day as integer) as pickup_day,
    cast(pickup_hour as integer) as pickup_hour,
    cast(pickup_weekday as varchar) as pickup_weekday,

    -- ids
    cast(vendorid as integer) as vendor_id,
    cast(pulocationid as integer) as pu_location_id,
    cast(dolocationid as integer) as do_location_id,

    -- trip facts
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    cast(trip_duration_min as double) as trip_duration_min,
    cast(trip_hours as double) as trip_duration_hour,
    cast(trip_speed_mph as double) as trip_speed_mph,

    -- monetary metrics
    cast(fare_amount as double) as fare_amount,
    cast(extra as double) as extra_amount,
    cast(mta_tax as double) as mta_tax_amount,
    cast(tip_amount as double) as tip_amount,
    cast(tolls_amount as double) as tolls_amount,
    cast(improvement_surcharge as double) as improvement_surcharge,
    cast(total_amount as double) as total_amount,
    cast(congestion_surcharge as double) as congestion_surcharge,
    cast(airport_fee as double) as airport_fee,
    cast(cbd_congestion_fee as double) as cbd_congestion_fee,
    cast(fare_per_mile as double) as fare_per_mile,

    -- standardized dimensions
    cast(payment_type as integer) as payment_type_code,
    cast(payment_type_desc as varchar) as payment_type,
    cast(ratecodeid as integer) as rate_code_id,
    cast(rate_code_desc as varchar) as rate_code,
    cast(store_and_fwd_flag as varchar) as store_and_forward_flag_raw,
    cast(store_and_fwd_flag_std as varchar) as store_and_forward_flag,

    -- flags
    cast(is_airport_trip as boolean) as is_airport_trip,
    cast(is_valid_trip as boolean) as is_valid_trip,

    -- data quality flags
    cast(dq_missing_pickup as boolean) as dq_missing_pickup,
    cast(dq_missing_dropoff as boolean) as dq_missing_dropoff,
    cast(dq_missing_pu_location as boolean) as dq_missing_pu_location,
    cast(dq_missing_do_location as boolean) as dq_missing_do_location,
    cast(dq_missing_trip_distance as boolean) as dq_missing_trip_distance,
    cast(dq_missing_fare_amount as boolean) as dq_missing_fare_amount,
    cast(dq_missing_total_amount as boolean) as dq_missing_total_amount,
    cast(dq_missing_passenger_count as boolean) as dq_missing_passenger_count,
    cast(dq_invalid_trip_distance as boolean) as dq_invalid_trip_distance,
    cast(dq_invalid_fare_amount as boolean) as dq_invalid_fare_amount,
    cast(dq_invalid_total_amount as boolean) as dq_invalid_total_amount,
    cast(dq_invalid_passenger_count as boolean) as dq_invalid_passenger_count,
    cast(dq_invalid_duration as boolean) as dq_invalid_duration,
    cast(dq_invalid_distance_outlier as boolean) as dq_invalid_distance_outlier,
    cast(dq_invalid_fare_outlier as boolean) as dq_invalid_fare_outlier,
    cast(dq_invalid_total_outlier as boolean) as dq_invalid_total_outlier,
    cast(dq_invalid_speed as boolean) as dq_invalid_speed,

    -- pickup zone
    cast(pu_borough as varchar) as pickup_borough,
    cast(pu_zone as varchar) as pickup_zone,
    cast(pu_service_zone as varchar) as pickup_service_zone,

    -- dropoff zone
    cast(do_borough as varchar) as dropoff_borough,
    cast(do_zone as varchar) as dropoff_zone,
    cast(do_service_zone as varchar) as dropoff_service_zone

from {{ source('silver', 'silver_trip_enriched') }}
where is_valid_trip = true