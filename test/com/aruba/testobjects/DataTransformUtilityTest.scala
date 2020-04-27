package com.aruba.testobjects

import com.aruba.tesutils.BaseSpec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class  DataTransformUtilityTest extends BaseSpec {
  val inputDf = spark.read.parquet("file:///G://Personalize//data//aruba_assignment//cleaned_data//")
  val checkfindDailyPeakHourDf = spark.read.option("header","true").csv("file:///G://Personalize//data//aruba_assignment//test_data//findDailyPeakHourDf.csv")
  
   def findDailyPeakHourDf(inputDf : DataFrame,spark :SparkSession) : DataFrame = {
    import spark.implicits._
    inputDf.createOrReplaceTempView("tempTbl")
    val outputDf = spark.sql("""select process_dt,process_hr,max(hourly_count) as max_hourly_count from (select to_date(process_dt) as process_dt,hour(process_dt) as process_hr,count(*) as hourly_count from (select tpep_pickup_datetime as process_dt from tempTbl union select tpep_dropoff_datetime as process_dt from tempTbl) c group by 1,2) d group by 1,2 order by 1,2""")
    spark.catalog.dropTempView("tempTbl")
    outputDf
  }
  
  test("findDailyPeakHourDfSchema"){
    assertSchemaEquals(inputDf.schema,checkfindDailyPeakHourDf.schema)
  }
  test("testData"){
    assertDataFrameEquals(inputDf, checkfindDailyPeakHourDf, "process_dt")
  }
}