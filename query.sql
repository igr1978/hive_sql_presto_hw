
# 1 - динамика количества поездок помесячно

SELECT year_month, month_trips, 
	(month_trips - LAG(month_trips, 1) OVER (ORDER BY year_month)) as lag_trips 
FROM (
	SELECT DATE_FORMAT(CAST(trip_start_timestamp as TIMESTAMP), '%Y-%m') as year_month, count(*) as month_trips
	FROM chicago_taxi_trips_parquet GROUP BY 1 ORDER BY year_month
	);


# 2 - топ-10 компаний по выручке

SELECT company, sum(trip_total) as total_sum FROM chicago_taxi_trips_parquet
GROUP BY company ORDER BY sum(trip_total) DESC LIMIT 10; 


# 3- доля поездок <5, 5-15, 16-25, 26-100 миль

SELECT (trip_04 / all_trips) * 100 as trip_miles_04,
	(trip_05_15 / all_trips) * 100 as trip_miles_05_15,
	(trip_16_25 / all_trips) * 100 as trip_miles_16_25,
	(trip_26_100 / all_trips) * 100 as trip_miles_26_100
FROM (
	SELECT sum(CASE WHEN trip_miles < 5 THEN 1 ELSE 0 end) as trip_04,
	sum(CASE WHEN trip_miles >= 5 AND trip_miles <= 15 THEN 1 ELSE 0 END) as trip_05_15,
	sum(CASE WHEN trip_miles >= 16 AND trip_miles <= 25 THEN 1 ELSE 0 END) as trip_16_25,
	sum(CASE WHEN trip_miles >= 26 AND trip_miles <= 100 THEN 1 ELSE 0 END) as trip_26_100,
	CAST(count(*) AS DOUBLE) all_trips
	FROM chicago_taxi_trips_parquet
	); 
	
	