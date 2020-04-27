package com.aruba.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.aruba.arch.DataTransformTrait
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Logger
import org.apache.hadoop.fs.{ FileSystem, Path }

object DataTransformUtility extends DataTransformTrait {

  val log = Logger.getLogger(getClass.getName)

  def dataTransformationTriggerUtility(preTransformDf: DataFrame, spark: SparkSession, configBasePath: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val findDailyPeakHourfileExists = fs.exists(new Path(configBasePath + "//findDailyPeakHour//"))
    val avgAmountPerPaymentTypefileExists = fs.exists(new Path(configBasePath + "//avgAmountPerPaymentType//"))
    val anomaliesAvgTripDistanceVendorwisefileExists = fs.exists(new Path(configBasePath + "//anomaliesAvgTripDistanceVendorwise//"))
    val anomalousTripVendorwisefileExists = fs.exists(new Path(configBasePath + "//anomalousTripVendorwise//"))
    val avgDistancePerRateCodeIdfileExists = fs.exists(new Path(configBasePath + "//avgDistancePerRateCodeId//"))
    val rankVendorIdByFareAmountfileExists = fs.exists(new Path(configBasePath + "//rankVendorIdByFareAmount//"))
    val categoryTripByTotalAmountfileExists = fs.exists(new Path(configBasePath + "//categoryTripByTotalAmount//"))
    try {
      log.info("Executing FindDailyPeakHour")
      if (!findDailyPeakHourfileExists) {
        val findDailyPeakHourDf = DataTransformUtility.findDailyPeakHourDf(preTransformDf, spark)
        findDailyPeakHourDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").option("compression", "gzip").csv(configBasePath + "//findDailyPeakHour//")
      }
      log.info("Executing AvgAmountPerPaymentType")
      if (!avgAmountPerPaymentTypefileExists) {
        val avgAmountPerPaymentTypeDf = DataTransformUtility.avgAmountPerPaymentTypeDf(preTransformDf, spark)
        avgAmountPerPaymentTypeDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").option("compression", "gzip").csv(configBasePath + "//avgAmountPerPaymentType//")
      }
      log.info("Executing AnomaliesAvgTripDistanceVendorwise")
      if (!anomaliesAvgTripDistanceVendorwisefileExists) {
        val anomaliesAvgTripDistanceVendorwiseDf = DataTransformUtility.anomaliesAvgTripDistanceVendorwiseDf(preTransformDf, spark)
        anomaliesAvgTripDistanceVendorwiseDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").option("compression", "gzip").csv(configBasePath + "//anomaliesAvgTripDistanceVendorwise//")
      }
      log.info("Executing AnomalousTripVendorwise")
      if (!anomalousTripVendorwisefileExists) {
        val anomalousTripVendorwiseDf = DataTransformUtility.anomalousTripVendorwiseDf(preTransformDf, spark)
        anomalousTripVendorwiseDf.coalesce(4).write.mode(SaveMode.Overwrite).option("header", "true").option("compression", "gzip").csv(configBasePath + "//anomalousTripVendorwise//")
      }
      log.info("Executing AvgDistancePerRateCodeId")
      if (!avgDistancePerRateCodeIdfileExists) {
        val avgDistancePerRateCodeIdDf = DataTransformUtility.avgDistancePerRateCodeIdDf(preTransformDf, spark)
        avgDistancePerRateCodeIdDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").option("compression", "gzip").csv(configBasePath + "//avgDistancePerRateCodeId//")
      }
      log.info("Executing RankVendorIdByFareAmount")
      if (!rankVendorIdByFareAmountfileExists) {
        val rankVendorIdByFareAmountDf = DataTransformUtility.rankVendorIdByFareAmountDf(preTransformDf, spark)
        rankVendorIdByFareAmountDf.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").option("compression", "gzip").csv(configBasePath + "//rankVendorIdByFareAmount//")
      }
      log.info("Executing CategoryTripByTotalAmountDf")
      if (!categoryTripByTotalAmountfileExists) {
        val categoryTripByTotalAmountDf = DataTransformUtility.categoryTripByTotalAmountDf(preTransformDf, spark)
        categoryTripByTotalAmountDf.coalesce(4).write.mode(SaveMode.Overwrite).option("header", "true").option("compression", "gzip").csv(configBasePath + "//categoryTripByTotalAmount//")
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing DataTransformationTriggerUtility ")
        System.exit(1)
    }
  }

  override def findDailyPeakHourDf(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      inputDf.createOrReplaceTempView("tempTbl")
      val outputDf = spark.sql("""select process_dt,process_hr,max(hourly_count) as max_hourly_count from (select to_date(process_dt) as process_dt,hour(process_dt) as process_hr,count(*) as hourly_count from (select tpep_pickup_datetime as process_dt from tempTbl union select tpep_dropoff_datetime as process_dt from tempTbl) c group by 1,2) d group by 1,2 order by 1,2""")
      spark.catalog.dropTempView("tempTbl")
      outputDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing FindDailyPeakHour Transformation")
        System.exit(1)
        null
    }
  }

  override def avgAmountPerPaymentTypeDf(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      inputDf.createOrReplaceTempView("tempTbl")
      val outputDf = spark.sql("""select payment_type,avg(sum_total_amount) as avg_amount from (select payment_type,to_date(tpep_pickup_datetime) as process_dt,sum(total_amount) as sum_total_amount from tempTbl group by 1,2) c group by 1 order by 1""")
      spark.catalog.dropTempView("tempTbl")
      outputDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing AvgAmountPerPaymentType Transformation")
        System.exit(1)
        null
    }
  }
  override def anomaliesAvgTripDistanceVendorwiseDf(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      inputDf.createOrReplaceTempView("tempTbl")
      val outputDf = spark.sql("""select VendorID,avg(sum_distance) avg_distance from (select VendorID,to_date(tpep_pickup_datetime) as process_dt, sum(trip_distance) as sum_distance from tempTbl group by 1,2) c group by 1 order by 1""")
      spark.catalog.dropTempView("tempTbl")
      outputDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing AnomaliesAvgTripDistanceVendorwise Transformation")
        System.exit(1)
        null
    }
  }

  override def anomalousTripVendorwiseDf(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      val modinDf = inputDf.withColumn("process_dt", expr("to_date(tpep_pickup_datetime)"))
      modinDf.createOrReplaceTempView("tempTbl")
      val threeDaysAvgDf = spark.sql("""SELECT distinct VendorID,process_dt, mean(trip_distance) OVER (PARTITION BY VendorID ORDER BY CAST(process_dt AS timestamp) RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND CURRENT ROW) AS mean FROM tempTbl""")
      threeDaysAvgDf.createOrReplaceTempView("tempAvg")
      var outputDf = spark.sql("""select a.*,b.mean as avg_trip_distance from tempTbl a left join tempAvg b on a.VendorID=b.VendorID and a.process_dt=b.process_dt where a.trip_distance>b.mean""")
      spark.catalog.dropTempView("tempTbl")
      spark.catalog.dropTempView("tempAvg")
      outputDf = outputDf.drop("process_dt")
      outputDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing AnomalousTripVendorwise Transformation")
        System.exit(1)
        null
    }
  }

  override def avgDistancePerRateCodeIdDf(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      inputDf.createOrReplaceTempView("tempTbl")
      val outputDf = spark.sql("""select RatecodeID,avg(trip_distance) as avg_trip_distance from tempTbl group by 1""")
      spark.catalog.dropTempView("tempTbl")
      outputDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing AvgDistancePerRateCodeId Transformation")
        System.exit(1)
        null
    }
  }

  override def rankVendorIdByFareAmountDf(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      inputDf.createOrReplaceTempView("tempTbl")
      val outputDf = spark.sql("""select *, rank() over (partition by process_dt order by fare_amt desc) from (select VendorID,to_date(tpep_pickup_datetime) as process_dt,sum(fare_amount) as fare_amt from tempTbl group by 1,2) c order by process_dt""")
      spark.catalog.dropTempView("tempTbl")
      outputDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing RankVendorIdByFareAmountDf Transformation")
        System.exit(1)
        null
    }
  }

  override def categoryTripByTotalAmountDf(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    try {
      val outputDf = inputDf.withColumn("category", expr("case when total_amount>=30.00 then 'high' when total_amount>=16.00 and total_amount<30.00 then 'medium' when total_amount<16.00 and total_amount>0.00 then 'low' else 'low' end"))
      outputDf
    } catch {
      case t: Throwable =>
        t.printStackTrace();
        log.error("Failed executing CategoryTripByTotalAmountDf Transformation")
        System.exit(1)
        null
    }
  }
}