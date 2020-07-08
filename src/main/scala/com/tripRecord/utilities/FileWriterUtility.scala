package com.tripRecord.utilities

import org.apache.spark.sql.DataFrame

object FileWriterUtility {

  def printRecord(df: DataFrame): Unit = {
    df.show(20, false)
  }

  def writeFiles(df: DataFrame, destPath: String, destFileType: String, numPartitions: Int): Unit = {
    df.coalesce(numPartitions).write.option("header", true).mode("append").format(s"$destFileType").save(s"$destPath")
  }
}
