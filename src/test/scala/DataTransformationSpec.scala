import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.scalatest.{FlatSpec, MustMatchers, ParallelTestExecution}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.tripRecord.processing.PreProcessingCheck
import com.tripRecord.transformation.DataTransformation
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.col

class DataTransformationSpec extends FlatSpec with MustMatchers with DataFrameSuiteBase with ParallelTestExecution{

  /*  override val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

  def getTimestamp(x:String) : Timestamp = {
    val format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aaa")
    if (x.toString() == "")
      return null
    else {
      val d = format.parse(x);
      val t = new Timestamp(d.getTime());
      return t
    }
  }

   val DF = spark.read
     .schema(Encoders.product[Record].schema)
     .csv(this.getClass.getClassLoader.getResource("Taxi.csv").getPath)*/

 // val DF = spark.read.option("header", true).option("dateFormat", "MM/dd/yyyy hh:mm:ss aaa").option("inferSchema", true)
 //   .csv(this.getClass.getClassLoader.getResource("Taxi.csv").getPath)
  val sourceData = Seq(
    ("1","2018-01-01 00:21:05","2018-01-01 00:41:05","1","41","24","2","4.5","5.8"),
    ("1","2018-01-01 00:44:55","2018-01-01 00:59:55","1","239","140","2","14","15.3"), // row from 2-5 are duplicate to 6-8
    ("1","2018-01-01 00:08:26","2018-01-01 00:28:26","1","262","141","1","6","8.3"),
    ("2","2018-01-01 00:24:47","2018-01-01 00:54:47","1","224","79","2","7.5","8.8"),
    ("1","2018-01-01 00:44:55","2018-01-01 00:59:55","1","239","140","2","14","15.3"),
    ("1","2018-01-01 00:08:26","2018-01-01 00:28:26","1","262","141","1","6","8.3"),
    ("2","2018-01-01 00:24:47","2018-01-01 00:54:47","1","224","79","2","7.5","8.8"),
    ("1","2018-01-04 00:44:55","2018-01-04 00:59:55","1","239","140","2","14","15.3"),
    ("1","2018-01-02 00:08:26","2018-01-02 00:28:26","1","262","141","1","6","8.3"),
    ("2","2018-01-03 00:24:47","2018-01-03 00:54:47","1","224","79","2","7.5","8.8"),

  )
  import spark.implicits._
  val DF: DataFrame = sourceData.toDF("vendorID","PickupDatetime","DropOffDatetime","rateCodeID","PULocationID","DOLocationID","paymentType","fareAmount","totAmount")



  val dataTransformation = new DataTransformation(spark)
  val preProcessing = new PreProcessingCheck(spark)

  "method" should "return distinct rows" in {
    val dfnew=DF.count() - DF.dropDuplicates().count()
    preProcessing.deDuplicate(DF).count() must be (dfnew)
  }

  "method" should "return masked value for distance column" in {
    val maskedColumn = Seq(col("PULocationID"),col("DOLocationID"))
    val result = preProcessing.anonymise(DF,maskedColumn).select("PULocationID","DOLocationID").dropDuplicates()
    val expected = Seq(0,0).toDF("PULocationID","DOLocationID")
    assertDataFrameEquals(expected,result)
  }
  "method" should "return 6 rows for payment Type" in {
    dataTransformation.avgTotalPerPaymentType(DF).count() must be (6)
  }

  "method" should "return only one row for peak Time" in{
    dataTransformation.peakTime(DF).count() must be (3)
  }

  "method" should "return avg trip per Vendor Id" in{
    dataTransformation.anomaliesVendor(DF).show(10)
  }
  "method" should "return rank vendorId based of fare amount" in{
    dataTransformation.rnkVendorFareAmt(DF).show(20)
  }
  "method" should "return categorised trip from high to low" in {
    dataTransformation.categoriseTripHighMediumLow(DF).show(10)
  }
}
