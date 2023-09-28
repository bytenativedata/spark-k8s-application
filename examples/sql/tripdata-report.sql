-- a runnable sql job
-- requied: a csv under s3a://spark-dwh/csv/

drop table if exists tripdata_spark;

CREATE TABLE tripdata_spark (
    VendorID bigint,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count bigint,
    trip_distance double,
    RatecodeID bigint,
    store_and_fwd_flag String,
    dropoff_longitude double,
    dropoff_latitude double,
    payment_type bigint,
    fare_amount double,
    extra double,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    improvement_surcharge double,
    total_amount double,
	congestion_surcharge double
)
USING CSV
OPTIONS (path "s3a://spark-dwh/csv/",
        delimiter ",",
        header "true")
        ;

drop table if exists tripdata_spark_report;

CREATE TABLE tripdata_spark_report
USING CSV
OPTIONS (path "s3a://spark-dwh/tripdata/tripdata_spark_report/",
        delimiter ",",
        header "true")
AS 
select 
	dayofweek(tpep_pickup_datetime) day_of_week,
	avg(total_amount) avg_amount,
	avg(trip_distance) avg_trip_distance,
	sum(passenger_count) total_passengers
from tripdata_spark
group by dayofweek(tpep_pickup_datetime);



select 
	dayofweek(tpep_pickup_datetime) ,
	avg(total_amount),
	avg(trip_distance),
	sum(passenger_count)
from tripdata_spark
group by dayofweek(tpep_pickup_datetime);

select * from tripdata_spark_report;