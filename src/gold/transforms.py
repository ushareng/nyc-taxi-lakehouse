from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def daily_revenue(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("year", "month", "pickup_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
        .orderBy("pickup_date")
    )


def zone_demand(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("year", "month", "pu_location_id", "pickup_borough", "pickup_zone", "pickup_hour")
        .agg(F.count("*").alias("trips"))
        .withColumnRenamed("pu_location_id", "location_id")
        .withColumnRenamed("pickup_borough", "borough")
        .withColumnRenamed("pickup_zone", "zone")
        .withColumnRenamed("pickup_hour", "hour")
    )


def payment_mix(silver: DataFrame) -> DataFrame:
    w = Window.partitionBy("year", "month")
    return (
        silver.groupBy("year", "month", "payment_type", "payment_type_desc")
        .agg(F.count("*").alias("trips"))
        .withColumn("share_of_trips", F.round(F.col("trips") / F.sum("trips").over(w), 4))
    )
