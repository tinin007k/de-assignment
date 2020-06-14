package com.triprecord.datalab.processes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ProcessCategorization {
  def categoriseOnAmount(inputDf: DataFrame): DataFrame = {
    val resultDf = inputDf.withColumn("Trip_Category",
      when(col("total_amount") < 10, "Low")
        .when(col("total_amount") > 30, "high")
        .otherwise("Medium")
    )
    resultDf
  }
}
