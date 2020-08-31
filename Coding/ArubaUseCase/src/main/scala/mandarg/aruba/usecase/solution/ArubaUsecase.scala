package mandarg.aruba.usecase.solution

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object ArubaUsecase extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    //We need the command line argument for data file path.
    //Check here. If not provided then log an error and exit the program
    if (args.length == 0) {
      logger.error("Usage: Provide the input data path")
      System.exit(1)
    }

    logger.info("Starting Spark application")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    //If required, you can use following line to see the configs set on console
    //logger.info("spark.conf=" + spark.conf.getAll.toString())

    //Define the schema for dataframe
    val tripdataRawSchema = "VendorID INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, " +
      "passenger_count INT, trip_distance FLOAT, RatecodeID INT, store_and_fwd_flag STRING, PULocationID INT, " +
      "DOLocationID INT, payment_type INT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, " +
      "tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT"

    //Load the dataframe
    val tripdataRaw = loadTripdata(spark, args(0), tripdataRawSchema)
    logger.info("Dataframe read from input file is successful")

    logger.info("Data pre-processing starts")

    //1. Anonymise location sensitive information
    //I have identified that - PULocationID and DOLocationID columns contains the location sensitive information.
    //I would drop these columns from dataframe to Anonymise location sensitive information as follows
    val tripdataLocationAnonymised = tripdataRaw.drop("PULocationID", "DOLocationID")

    //2. Detect data loss during ingestion
    //As the data file is directly available I am skipping this step

    //3. De-duplicate the data before transformations
    val tripdataRawDeDuplication = tripdataRaw.dropDuplicates("VendorID","tpep_pickup_datetime", "tpep_dropoff_datetime",
      "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
      "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount")
    val NumberOfDuplicateRecords = tripdataRaw.count() - tripdataRawDeDuplication.count()
    logger.info("DeDuplication done. Number of duplicate records found: "+NumberOfDuplicateRecords)

    logger.info("Data pre-processing ends")

    //Lets find the average total amount per payment type
    val dfAverageTotalAmountPerPayment = getAverageTotalAmountPerPayment(tripdataRawDeDuplication)
    dfAverageTotalAmountPerPayment.show()

    //Lets find the anomalies in the average trip distance per Vendor Id
    //I have flagged the records in below df with column 'isAnomalous' True and False
    val dfAnomaliesInAverageTripDistancePerVendor=getAnomaliesInAverageTripDistancePerVendor(tripdataRawDeDuplication)
    dfAnomaliesInAverageTripDistancePerVendor.show()
    logger.info("Count of total records: "+tripdataRaw.count()+" Count of Records which are flagged anomalous: "+dfAnomaliesInAverageTripDistancePerVendor.filter(col("isAnomalous")==="True").count())

    //Lets find the average trip distance per RateCodeId
    val dfAverageTripDistancePerRatecard=getAverageTripDistancePerRatecard(tripdataRawDeDuplication)
    dfAverageTripDistancePerRatecard.show()

    //Lets rank the Vendor Id/per day based on the fare amount
    val dfRankOfVendorsPerDay = getRankOfVendorsPerDay(tripdataRawDeDuplication)
    dfRankOfVendorsPerDay.show()

    //Lets categorise the trips based on its total amount to high, medium and low
    val dfCategoriesBasedOnTripes = getCategoriesBasedOnTripes(tripdataRawDeDuplication)
    dfCategoriesBasedOnTripes.show()

    logger.info("Stopping Spark application")
    spark.stop()
  }

  def getCategoriesBasedOnTripes(df: DataFrame): DataFrame={
    df
      .withColumn("category",lit("high"))
      .withColumn("category",expr(
      """
        |case when total_amount > 30.00 then category
        |when total_amount < 15.00 then replace(category,'high','low')
        |else replace(category,'high','medium')
        |end
        |""".stripMargin
    ))
  }

  def getRankOfVendorsPerDay(df: DataFrame): DataFrame={
    //Add a column to extract date out of timestamp
    val dfAddedWithDateColumn=df.withColumn("tpep_pickup_date",to_date(col("tpep_pickup_datetime"),"yyyy-MM-dd"))

    //Lets create a window spec
    val windowSpec = Window.partitionBy("VendorID", "tpep_pickup_date").orderBy(desc("fare_amount")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val dfAfterGroupBy=dfAddedWithDateColumn.groupBy("tpep_pickup_date","VendorID")
      .agg(sum("fare_amount").as("fare_amount"))
      .select("tpep_pickup_date","VendorID", "fare_amount")

    dfAfterGroupBy.withColumn("Rank",dense_rank().over(windowSpec)
    ).show(150)

    //df.withColumn("tpep_pickup_date",to_date(col("tpep_pickup_datetime"),"yyyy-MM-dd")).groupBy("VendorID","tpep_pickup_date").sum("fare_amount")
    return df
  }

  def getAverageTripDistancePerRatecard(df: DataFrame): DataFrame={
    df.groupBy("RatecodeID")
      .agg(avg("trip_distance").as("Average trip distance"))
      .select("RatecodeID","Average trip distance")
  }

  def getAnomaliesInAverageTripDistancePerVendor(df: DataFrame): DataFrame={
    //First lets calculate the average per vendor
    val dfAveragePerVendor=df
      .groupBy("VendorID")
      .agg(mean("trip_distance").as("averageTripDistancePerVendor"))

    //Lets join above df with input df, so as to get a extra column which shows average per vendor
    //then label records anomalous, if trip_distance>averageTripDistancePerVendor
    val joinExpr = dfAveragePerVendor.col("VendorID")===df.col("VendorID")
    val joinType="inner"
    dfAveragePerVendor.join(df,joinExpr,joinType)
      .drop(dfAveragePerVendor.col("VendorID"))
      .withColumn("isAnomalous",lit("True"))
      .withColumn("isAnomalous",expr(
        """
          |case when trip_distance > averageTripDistancePerVendor then isAnomalous
          |when trip_distance==0.0 then isAnomalous
          |else replace(isAnomalous,'True','False')
          |end
          |""".stripMargin
      ))
  }

  def getAverageTotalAmountPerPayment(df: DataFrame): DataFrame={
    df.groupBy("payment_type")
      .agg(avg("total_amount").as("Average total amount"))
      .select("payment_type","Average total amount")
  }

  //Read the spark config object from properties file
  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    //Below two lines is a fix for Scala 2.11
    //import scala.collection.JavaConverters._
    //props.asScala.foreach(kv => sparkAppConf.set(kv._1, kv._2))
    sparkAppConf
  }

  //Read the given input csv file
  def loadTripdata(spark: SparkSession, dataFile: String, schema: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("path", dataFile)
      .option("mode","FAILFAST")
      .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
      .schema(schema)
      .load()
  }
}