package com.triprecord.datalab.processes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object ProcessPeakTime {

  def findPeakTime(inputDf: DataFrame): DataFrame = {

    val df2 = inputDf.withColumn("pickup_date", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
      .withColumn("pickup_time", hour(col("tpep_pickup_datetime")))
      .withColumn("dropoff_date", to_date(col("tpep_dropoff_datetime"), "yyyy-MM-dd"))
      .withColumn("dropoff_time", hour(col("tpep_dropoff_datetime")))
      .select("pickup_date", "pickup_time", "dropoff_date", "dropoff_time")


    val pickupdf = df2.groupBy("pickup_date", "pickup_time")
      .agg(count("pickup_time").alias("pcounts"))
      .withColumnRenamed("pickup_date", "trip_date")
      .withColumnRenamed("pickup_time", "trip_hour")

    val dropoffdf = df2.groupBy("dropoff_date", "dropoff_time")
      .agg(count("dropoff_time").alias("dcounts"))
      .withColumnRenamed("dropoff_date", "trip_date")
      .withColumnRenamed("dropoff_time", "trip_hour")

    val joindf = pickupdf.join(dropoffdf, Seq("trip_date", "trip_hour"), "outer")

    val tripcountdf = joindf.withColumn("trip_counts", expr("pcounts + dcounts"))

    val resultdf = tripcountdf
      .withColumn("rownum", row_number().over(Window.partitionBy("trip_date").orderBy(col("trip_counts").desc))).where(expr("rownum == 1"))
      .withColumnRenamed("trip_hour", "peak_hour")
      .drop("pcounts", "dcounts", "trip_counts", "rownum")
      .orderBy("trip_date")

    resultdf

  }

}
