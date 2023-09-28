
-- type specified

drop table if exists spark_catalog.default.tripdata_spark;

CREATE TABLE spark_catalog.default.tripdata_spark (
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
   
----------------
-- iceberg hive

use ib_hive_cat.default;

drop table if exists tripdata;

CREATE TABLE ib_hive_cat.default.tripdata
    USING parquet
    PARTITIONED BY (day_of_week)
    CLUSTERED BY (VendorID) INTO 4 buckets
    AS SELECT *, 
   		dayofweek(tpep_pickup_datetime) day_of_week 
   	FROM spark_catalog.default.tripdata_spark
   	-- ordering to avoid file close error
    order by day_of_week, VendorID;
   
select * from tripdata;
 
create database if not exists ib_hive_cat.report;

use ib_hive_cat.report;

drop table if exists tripdata_report;

CREATE TABLE tripdata_report
USING parquet
AS 
select 
	dayofweek(tpep_pickup_datetime) day_of_week,
	avg(total_amount) avg_amount,
	avg(trip_distance) avg_trip_distance,
	sum(passenger_count) total_passengers
from ib_hive_cat.default.tripdata
group by dayofweek(tpep_pickup_datetime);
   
select * from tripdata_report;

