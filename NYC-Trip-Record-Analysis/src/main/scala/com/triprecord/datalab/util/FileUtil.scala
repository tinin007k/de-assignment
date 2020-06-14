package com.triprecord.datalab.util

import com.triprecord.datalab.environment.SparkEnvironment._
import org.apache.spark.sql.DataFrame

import scala.util.Try

object FileUtil {

  def readFiles(srcPath: String, srcType: String) : Try[DataFrame] = {
    srcType match {
      case "csv" => Try(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(s"$srcPath"))
    }
  }

  def printRecord(df: DataFrame): Unit = {
    df.show(20, false)
  }

  def writeFiles(df: DataFrame, destPath: String, destFileType: String, numPartitions: Int): Try[Unit] = {
    Try(df.coalesce(numPartitions).write.option("header", true).mode("append").format(s"$destFileType").save(s"$destPath"))
  }

}
