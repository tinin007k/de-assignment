package com.tripRecord.processing

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import org.apache.spark.sql.functions.{lit}

class PreProcessingCheck(spark: SparkSession) {

  def anonymise(DF: DataFrame, cols: Seq[Column]): DataFrame={
    cols.foreach(p=> DF.withColumn(p.toString(),lit(0)))
    println("Masking Done")
    DF
  }

  def deDuplicate(DF:DataFrame):DataFrame={
    val distinctRec = DF.dropDuplicates()
    val duprecords = DF.count() - distinctRec.count()
    println(s"No of Duplicate records  $duprecords" )
    distinctRec
  }

  def DataLossCheck(Df:DataFrame):Either[String,DataFrame] = ???

}