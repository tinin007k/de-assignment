package com.aruba.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode

object CommonUtility {
  def flushData(spark: SparkSession): Unit = {
    val rdd = spark.sparkContext.parallelize(Seq("FlushData")).map(r => Row(r))
    val schema = org.apache.spark.sql.types.StructType(Array(StructField("FlushData", org.apache.spark.sql.types.StringType, true)))
    val flushDf = spark.createDataFrame(rdd, schema)
    flushDf.write.mode(SaveMode.Overwrite).csv("/FlushData/FlushData.csv")
  }
}