WITH daily_weather AS(

SELECT 
    DATE(TIME) as daily_weather,
    weather,
    temp,
    pressure,
    HUNIDIT as Humidity,
    clouds
FROM {{ source('demo', 'weather') }}

),

daily_weather_agg as (
    SELECT
        dw.daily_weather,
        dw.weather,
        round(avg(dw.temp),2) as avg_temp,
        round(avg(dw.pressure),2) as avg_pressure,
        round(avg(dw.Humidity),2) as avg_humidity,
        round(avg(dw.clouds),2) as avg_clouds
        -- ROW_NUMBER() OVER (PARTITION BY dw.daily_weather ORDER BY COUNT(dw.weather) DESC) as row_number
    FROM daily_weather dw
    GROUP BY dw.daily_weather,dw.weather
    qualify ROW_NUMBER() OVER (PARTITION BY dw.daily_weather ORDER BY COUNT(dw.weather) DESC) = 1
    
)

SELECT * FROM daily_weather_agg
ORDER BY daily_weather
