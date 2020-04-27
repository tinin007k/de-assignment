package com.aruba.driver

import org.apache.spark.sql.SparkSession
import com.aruba.utils.CleaningUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import com.aruba.utils.CommonUtility
import org.apache.spark.sql.SaveMode
import com.aruba.utils.DataTransformUtility
import org.apache.log4j.Logger

object ArubaAssignmentDriver {

  val log = Logger.getLogger(getClass.getName)
	def main(args: Array[String]): Unit = {
	  //Require to running on Windows Machine 
      //System.setProperty("hadoop.home.dir", "C://winutils")
			val spark = SparkSession.builder().master("yarn").getOrCreate()
			//val configBasePath = "file:///G://Personalize//data//aruba_assignment"
			//val yearOfProcessing = 2018
			val configBasePath = String.valueOf(args(0).trim())
			val yearOfProcessing = String.valueOf(args(1).trim()).toInt
			
			if (args == null || args.isEmpty || args.length != 2) {
      log.error("Invalid number of arguments passed.")
      log.error("Arguments Usage: <Base File path> <Year Of Processing>")
      log.error("Stopping the flow")
      System.exit(1)
    }
	    
	    log.info("Reading the Source input file-Absolute path: " +configBasePath+"//src_data//")
			val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
			val inputDf = spark.read.option("header","true").csv(configBasePath+"//src_data//")
			log.info("Source Data Count: "+ inputDf.count())
			val fileExists = fs.exists(new Path(configBasePath+"//cleaned_data//"))
			if(!fileExists){
			val cleanedDf = CleaningUtils.dataPreprocessingUtility(inputDf, spark,yearOfProcessing)
			cleanedDf.write.mode(SaveMode.Overwrite).parquet(configBasePath+"//cleaned_data//")
			CommonUtility.flushData(spark)
			}
			val preTransformDf = spark.read.parquet(configBasePath+"//cleaned_data//")
			log.info("Difference between Source Data Count and Cleaned Data Count: "+ (inputDf.count()-preTransformDf.count()))
			DataTransformUtility.dataTransformationTriggerUtility(preTransformDf, spark, configBasePath)
	}
}