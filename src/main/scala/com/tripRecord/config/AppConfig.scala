package com.tripRecord.config
import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  private val fileName = new File("Users\\viveksingh\\Downloads\\HpTaxiProblem\\src\\main\\resources\\application.conf")
  private val config: Config = ConfigFactory.parseFile(fileName);
  lazy val srcPath: String = config.getString("triprecord.source")
  lazy val srcFileType: String = config.getString("triprecord.srcFileType")
  lazy val destPath: String = config.getString("triprecord.destination")
  lazy val destFileType: String = config.getString("triprecord.destFileType")
  lazy val numPartitions: Int = config.getInt("triprecord.numPartitions")
}
