package com.triprecord.datalab.preprocess

import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSpec, Matchers}
import com.triprecord.datalab.environment.SparkEnvironment._
import com.triprecord.datalab.preprocess.PreProcess._

class PreProcessTest extends FunSpec with Matchers{

  trait FixtureSrcData{
    import spark.implicits._
    val sourceData = Seq(
      ("1","2018-01-01 00:21:05","1","41","24","2","4.5","5.8"),
      ("1","2018-01-01 00:44:55","1","239","140","2","14","15.3"), // row from 2-5 are duplicate to 6-8
      ("1","2018-01-01 00:08:26","1","262","141","1","6","8.3"),
      ("2","2018-01-01 00:24:47","1","224","79","2","7.5","8.8"),
      ("1","2018-01-01 00:44:55","1","239","140","2","14","15.3"),
      ("1","2018-01-01 00:08:26","1","262","141","1","6","8.3"),
      ("2","2018-01-01 00:24:47","1","224","79","2","7.5","8.8"),
      ("2","2018-01-01 00:20:22","5","140","257","2","33.5","34.8"),
      ("1","2018-01-01 00:09:18","1","246","239","1","12.5","16.55"),
      ("2","2018-01-01 00:29:29","1","143","143","2","4.5","5.8"),
      ("3","2018-01-01 00:38:08","1","50","239","1","9","12.35"), // wrong records starts from here size 7
      ("3","2018-01-01 00:49:29","1","239","238","1","4","6.3"),
      ("1","2017-01-01 00:56:38","1","238","24","1","5.5","8.5"),
      ("1","2018-03-01 00:17:04","2","170","170","2","5.5","6.8"),
      ("1","2018-01-01 00:41:03","8","162","229","1","5.5","8.15"),
      ("1","2018-01-01 00:52:54","1","141","113","9","16.5","17.8"),
      ("2","2018-01-01 00:17:54","1","137","224","7","5.5","6.8")
    )

    val sourceDataDf: DataFrame = sourceData.toDF("VendorID","tpep_pickup_datetime","RatecodeID","PULocationID","DOLocationID","payment_type","fare_amount","total_amount")
  }

  describe("PreProcess.anonymiseSensativeInfo"){
    it("should anonymise PULocationID and DOLocationID"){
      new FixtureSrcData {
        val anonymiseDF: DataFrame = sourceDataDf.anonymiseSensativeInfo
        anonymiseDF.columns.contains("PULocationID") shouldBe false
        anonymiseDF.columns.contains("DOLocationID") shouldBe false
        anonymiseDF.columns.contains("EncPULocationID") shouldBe true
        anonymiseDF.columns.contains("EncDOLocationID") shouldBe true
      }
    }
  }

  describe("PreProcess.cleanWrongData"){
    it("should remove data which is not as per data dictionary"){
      new FixtureSrcData {
        val cleanedDF: DataFrame = sourceDataDf.cleanWrongData
        cleanedDF.count() shouldBe sourceData.size-7
      }
    }
  }

  describe("PreProcess.deduplicate"){
    it("should remove duplicates"){
      new FixtureSrcData {
        val dedupDF: DataFrame = sourceDataDf.deduplicate
        dedupDF.count() shouldBe sourceData.size-3
      }
    }
  }

}
