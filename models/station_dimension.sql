WITH BIKE as(

SELECT
    DISTINCT
    START_STATIO_ID as station_id,
    start_station_name,
    start_lat,
    start_lng
FROM {{ ref('stg_bike') }}
WHERE RIDE_ID != 'ride_id'
)

SELECT
* 
FROM BIKE