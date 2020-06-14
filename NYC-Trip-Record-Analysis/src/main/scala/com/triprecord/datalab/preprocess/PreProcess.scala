package com.triprecord.datalab.preprocess

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object PreProcess {
  implicit final class ImplicitClass(val df: DataFrame){

    // encrypt function
    val md5HashString = (s: String) => {
      import java.security.MessageDigest
      import java.math.BigInteger
      val md = MessageDigest.getInstance("MD5")
      val digest = md.digest(s.getBytes)
      val bigInt = new BigInteger(1, digest)
      val hashedString = bigInt.toString(16)
      hashedString
    }

    // method to encrypt location details
    def anonymiseSensativeInfo: DataFrame = {
      val encryptLoc = udf(md5HashString)
      df.withColumn("EncPULocationID", encryptLoc(col("PULocationID")))
        .withColumn("EncDOLocationID", encryptLoc(col("DOLocationID")))
        .drop("PULocationID", "DOLocationID")
    }

    // removing records data dictionary
    def cleanWrongData: DataFrame = {
      df.filter(expr("tpep_pickup_datetime >= '2018-01-01 00:00:00' and tpep_pickup_datetime <= '2018-01-31 23:59:59'"))
        .filter(expr("VendorID == 1 or VendorID == 2"))
        .filter(expr("RateCodeID >= 1 and RateCodeID <= 6"))
        .filter(expr("Payment_type >= 1 and Payment_type <= 6"))
    }

    // removing duplicate rows
    def deduplicate: DataFrame = {
      df.dropDuplicates()
    }

    def preprocess: DataFrame = {
      df.anonymiseSensativeInfo.cleanWrongData.deduplicate
    }

  }

}
