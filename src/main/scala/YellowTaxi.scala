import java.sql.Timestamp

import com.tripRecord.processing.PreProcessingCheck
import com.tripRecord.config.AppConfig
import com.tripRecord.transformation.DataTransformation
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext}
import org.apache.spark.sql.functions._
import com.tripRecord.utilities.FileWriterUtility._
import org.apache.log4j.Logger


case class Record (vendorId: Int,
                   PickupDatetime: Timestamp,
                   DropOffDatetime: Timestamp,
                   PassengerCount: Int,
                   tripDistance: Double,
                   rateCodeID : Int,
                   storeAndFwdFlag: String,
                   PULocationID : Int,
                   DOLocationID: Int,
                   paymentType: Int,
                   fareAmount: Double,
                   extra: Double,
                   mtaTax: Double,
                   tipAmount: Double,
                   tollsAmount: Double,
                   improvementSurcharges: Double,
                   totAmount: Double
                  )

object YellowTaxi extends App {
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate;

/*  val df = spark.read
    .schema(Encoders.product[Record].schema)
    .csv(
      this.getClass.getClassLoader
        .getResource("Macintosh HD\\Users\\viveksingh\\Downloads\\yellow_tripdata_2018-01.csv")
        .getPath
    )*/
  val df = spark.read
    .schema(Encoders.product[Record].schema)
    .format("csv").load("Users/viveksingh/Downloads/yellow_tripdata_2018-01.csv")

  val log = Logger.getLogger(getClass.getName)
  try{
    println(" starting PreValidation check")

    val maskedColumn = Seq(col("PULocationID"),col("DOLocationID"))
    val procCheck = new PreProcessingCheck(spark)
    val preDf1 = procCheck.anonymise(df,maskedColumn)
    val dataLossCheck = procCheck.DataLossCheck(preDf1)
    val Df2= procCheck.deDuplicate(procCheck.anonymise(df,maskedColumn))

    val dataTransformation = new DataTransformation(spark)
    log.info(" peak time of pickup/drops on a daily basis")
    val peakDF = dataTransformation.peakTime(Df2)
    printRecord(peakDF)
    writeFiles(peakDF,AppConfig.destPath,AppConfig.destFileType,AppConfig.numPartitions)

    log.info(" average total amount per payment type.")
    val totPerPaymentType = dataTransformation.avgTotalPerPaymentType(Df2)
    printRecord(totPerPaymentType)
    writeFiles(totPerPaymentType,AppConfig.destPath,AppConfig.destFileType,AppConfig.numPartitions)


    log.info(" anomalies in the average trip distance per Vendor Id.")
    val anomalies = dataTransformation.anomaliesVendor(Df2)
    printRecord(anomalies)
    writeFiles(anomalies,AppConfig.destPath,AppConfig.destFileType,AppConfig.numPartitions)

    log.info(" average trip distance per RateCodeId.")
    val avgTripRateCode = dataTransformation.avgTripRateCode(Df2)
    printRecord(avgTripRateCode)
    writeFiles(avgTripRateCode,AppConfig.destPath,AppConfig.destFileType,AppConfig.numPartitions)

    log.info(" Vendor Id/per day based on the fare amount.")
    val rnkVendorFare = dataTransformation.rnkVendorFareAmt(Df2)
    printRecord(rnkVendorFare)
    writeFiles(rnkVendorFare,AppConfig.destPath,AppConfig.destFileType,AppConfig.numPartitions)

    log.info(" trips based on its total amount to high, medium and low.")
    val categoriseTripHighMedLow = dataTransformation.categoriseTripHighMediumLow(Df2)
    printRecord(categoriseTripHighMedLow)
    writeFiles(categoriseTripHighMedLow,AppConfig.destPath,AppConfig.destFileType,AppConfig.numPartitions)

  }catch {case _=>log.info("Execution Failed with Error")}
  finally {
    spark.stop()
    log.info("Execution Completed")
  }
}