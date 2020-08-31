package mandarg.aruba.usecase.solution

import mandarg.aruba.usecase.solution.ArubaUsecase.loadTripdata
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ArubaUsecaseTest extends FunSuite with BeforeAndAfterAll{

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Aruba UseCase")
      .master("local[3]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Data File Loading") {
    val tripdataRawSchema = "VendorID INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, " +
      "passenger_count INT, trip_distance FLOAT, RatecodeID INT, store_and_fwd_flag STRING, PULocationID INT, " +
      "DOLocationID INT, payment_type INT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, " +
      "tolls_amount FLOAT, improvement_surcharge FLOAT, total_amount FLOAT"
    val dataFile= "data/yellow_tripdata_2018-01.csv"
    val sampleDF = loadTripdata(spark,dataFile,tripdataRawSchema)
    val rCount = sampleDF.count()
    assert(rCount==8759874, " record count should be 8759874")
  }

}
