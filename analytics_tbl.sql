CREATE OR REPLACE TABLE `uber-project-411414.uber_data_engineering_project.analytics_tbl` AS (
SELECT 
f.ID,
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime,
pick.pickup_longitude,
pick.pickup_latitude,
drop.dropoff_longitude,
drop.dropoff_latitude,
t.passenger_count,
t.trip_distance,
r.RatecodeID,
r.rate_code_name,
pay.payment_type,
pay.payment_name,
a.fare_amount,
a.extra,
a.mta_tax,
a.tip_amount,
a.tolls_amount,
a.improvement_surcharge,
a.total_amount
FROM `uber-project-411414.uber_data_engineering_project.fact_table` f
JOIN `uber-project-411414.uber_data_engineering_project.datetime_dim` d  ON f.datetime_id=d.datetime_id
JOIN `uber-project-411414.uber_data_engineering_project.dropoff_dim` drop  ON drop.dropoff_location_id=f.dropoff_location_id
JOIN `uber-project-411414.uber_data_engineering_project.pickup_dim` pick  ON pick.pickup_location_id=f.pickup_location_id  
JOIN `uber-project-411414.uber_data_engineering_project.ratecode_dim` r ON r.rate_code_id=f.rate_code_id  
JOIN `uber-project-411414.uber_data_engineering_project.payment_type_dim` pay ON pay.payment_id=f.payment_id
JOIN `uber-project-411414.uber_data_engineering_project.total_amount_dim` a ON a.total_amount_id=f.total_amount_id
JOIN `uber-project-411414.uber_data_engineering_project.trip_details_dim` t ON t.trip_id=f.trip_id);