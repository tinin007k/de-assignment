package com.aruba.tesutils

import java.nio.file.{Files, Path}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TempFolderUtil extends Suite with BeforeAndAfterAll {
  private var tempDir: Path = _
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory(this.getClass.getSimpleName)
  }
  override protected def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(tempDir.toFile)
  }
  protected def withTempBucket(f: String => Unit): Unit = {
    val dir = Files.createTempDirectory(tempDir, "bucket-path")
    try f(dir.toString) finally {
      FileUtils.deleteDirectory(dir.toFile)
    }
  }
}