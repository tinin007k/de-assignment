package com.aruba.tesutils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, SQLContext, SQLImplicits, SparkSession }
import org.scalatest._



abstract class BaseSpec extends FunSuite with TempFolderUtil with BeforeAndAfterEach with Matchers {
  var spark: SparkSession = _
  def assertDataFrameEquals(expected: DataFrame, actual: DataFrame, sortColumns: String*): Assertion = {
    assertSchemaEquals(expected.schema, actual.schema)
    val orderedColumns = expected.schema.fieldNames
    val expectedRows   = expected.sort(sortColumns.map(expected.apply): _*).collect()
    val actualRows = actual
      .selectExpr(orderedColumns: _*)
      .sort(sortColumns.map(actual.apply): _*)
      .collect()
    assert(actualRows === expectedRows, "dataFrames are not equal")
  }
  def assertSchemaEquals(expected: StructType, actual: StructType): Assertion = {
    val expectedFields = expected.fields.map(field => (field.name, field.dataType)).toMap
    val actualFields   = actual.fields.map(field => (field.name, field.dataType)).toMap
    assert(actualFields === expectedFields, "schemas do not match")
  }
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark = SparkSession
      .builder()
      .appName("mw-testing")
      .master("local[4]")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", false)
      .config("spark.driver.allowMultipleContexts", "false")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.driver.memory", "2g")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }
  object testImplicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }
}