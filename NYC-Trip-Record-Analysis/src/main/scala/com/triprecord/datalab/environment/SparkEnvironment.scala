package com.triprecord.datalab.environment

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkEnvironment {
  @transient lazy val conf: SparkConf = new SparkConf().setAppName("NYC-TripRecord-Analysis")
    .setMaster("local[*]")
  @transient lazy val spark: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()
}
