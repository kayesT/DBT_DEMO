WITH CTE AS(

SELECT
t.*,
w.*
FROM {{ ref('trip_fact') }} t
left join {{ ref('daily_weather') }} w
ON t.TRIP_DATE = w.daily_weather

ORDER BY TRIP_DATE DESC   
)

SELECT * FROM CTE