package com.tripRecord.transformation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class DataTransformation(spark: SparkSession) {
  def peakTime(DF:DataFrame): DataFrame = {

    import spark.implicits._
    val DfHour = DF.withColumn("startHour",hour(col("PickupDatetime" )))
      .withColumn("endHour", hour(col("DropOffDatetime")))
      .withColumn("startDate",(col("PickupDatetime")).cast(DateType))
    val win = Window.partitionBy($"Date").orderBy($"strtHourcnt")
    val df1 = DfHour.groupBy("Date","startHour")
      .agg(count("startHour").alias("strtHourcnt"))
      .select($"Date",$"startHour", rank.over(win).alias("rnk")).filter($"rnk"===1)
    val win2 = Window.partitionBy($"Date").orderBy($"endHour")
    val df2 = DfHour.groupBy("Date","endHour")
      .agg(count("endHour").alias("endHourcnt"))
      .select($"Date",$"endHour", rank.over(win).alias("rnk")).filter($"rnk"===1)
    val Df = df1.join(df2,$"Date"===$"Date", "inner").drop(df2("Date"))
    Df
  }
  def avgTotalPerPaymentType(DF:DataFrame): DataFrame = {
    DF.groupBy("paymentType").avg("totAmount").alias("AvgTotalAmountPerTrip")
  }
  def anomaliesVendor(DF:DataFrame): DataFrame = {
    val dayMean = DF.withColumn("startDate",(col("PickupDatetime")).cast(DateType))
      .groupBy("startDate").avg("tripDistance").alias("AvgbyDay")
      .withColumn("movingAvg",avg("AvgbyDay").over(Window.orderBy("startDate").rowsBetween(-2,0)))
    val df = DF.withColumn("startDate",(col("PickupDatetime")).cast(DateType))
      .groupBy("vendorId","startDate").avg("tripDistance").alias("vendorTripDay")
    import spark.implicits._
    val res = df.join(dayMean,$"startDate"===$"startDate", "inner")
      .drop(dayMean("startDate"))
      .filter($"VendorTripDay" <= $"movingAvg")
    res.select("startDate","vendorId","vendorTripDay","movingAvg")
  }
  def avgTripRateCode(DF:DataFrame): DataFrame = {
    val df = DF.withColumn("avgTripPerRateCode",avg("tripDistance").over(Window.partitionBy("rateCodeID")))
    df.select("rateCodeID","avgTripPerRateCode")
  }
  def rnkVendorFareAmt(DF:DataFrame): DataFrame = {
    val df = DF.withColumn("startDate",(col("PickupDatetime")).cast(DateType))
      .groupBy("startDate","vendorId").sum("fareAmount").alias("FareByVendorId")
      .withColumn("rnk",rank().over(Window.partitionBy("startDate").orderBy("FareByVendorId")))
    df.select("startDate","vendorId","rnk")
  }
  def categoriseTripHighMediumLow(DF:DataFrame): DataFrame ={
    import spark.implicits._
    val df = DF.withColumn("tripLevel", when($"tripDistance" between (1 , 15),"low")
      .when($"tripDistance" between(16 , 30) , "medium").otherwise("High"))
    df.select("vendorId","tripLevel")
  }
}