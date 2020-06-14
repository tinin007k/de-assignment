package com.triprecord.datalab.config

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  private val fileName = new File("C:\\Users\\Vivek\\IdeaProjects\\NYC-Trip-Record-Analysis\\src\\main\\resources\\application.conf")
  private val config: Config = ConfigFactory.parseFile(fileName);
  lazy val processName: String = config.getString("processName")
  lazy val srcPath: String = config.getString("triprecord.source")
  lazy val srcFileType: String = config.getString("triprecord.srcFileType")
  lazy val destPath: String = config.getString("triprecord.destination")
  lazy val destFileType: String = config.getString("triprecord.destFileType")
  lazy val numPartitions: Int = config.getInt("triprecord.numPartitions")
}
