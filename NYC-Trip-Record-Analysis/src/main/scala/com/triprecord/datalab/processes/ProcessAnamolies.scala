package com.triprecord.datalab.processes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ProcessAnamolies {
  def findAnamoliesTripDistance(inputDf: DataFrame) : DataFrame = {

    val dateDf = inputDf.withColumn("pickup_date", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
      .select("pickup_date", "trip_distance")

    val aggDf = dateDf.groupBy("pickup_date")
      .agg(sum("trip_distance").alias("trip_distance"),
        count("trip_distance").alias("trip_count"))
      .orderBy("pickup_date")

    val wspec = Window.orderBy("pickup_date")

    val winDf = aggDf.withColumn("tom_td", lead("trip_distance", 1).over(wspec))
      .withColumn("dayAfter_td", lead("trip_distance", 2).over(wspec))
      .withColumn("tom_tc", lead("trip_count", 1).over(wspec))
      .withColumn("dayAfter_tc", lead("trip_count", 2).over(wspec))
      .withColumn("next_3_days_avg",
        expr("(trip_distance + tom_td + dayAfter_td ) / ( trip_count + tom_tc + dayAfter_tc)"))
      .drop("trip_distance", "tom_td", "dayAfter_td", "trip_count", "tom_tc", "dayAfter_tc")

    val joindf = inputDf
      .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
      .join(winDf, Seq("pickup_date"))

    val resultDf = joindf.withColumn("anamolies",
      when(expr("trip_distance > next_3_days_avg"), "YES")
          .otherwise("NO")
      )

    resultDf
  }
}
