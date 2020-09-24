package com.github.music.of.the.ainur.almaren.http

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SaveMode, SparkSession}
import org.scalatest._
import org.apache.spark.sql.functions._
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTP.HTTPImplicit

case class Data(id:Long, name:String, age:Int, phone:String)

class Test extends FunSuite with BeforeAndAfter {
  val almaren = Almaren("bigQuery-almaren")

  val spark: SparkSession = almaren.spark
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  import spark.implicits._
  val df = List(
    Data(1,"daniel",30,"555456123"),
    Data(1,"daniel",30,"555456999"),
    Data(2,"peter",25,"555456123"),
    Data(3,"sara",45,"555454677"),
    Data(4,"alice",88,"555555123")
  ).toDF

  df.createOrReplaceTempView("foo")

  val newSession = () => requests.Session(headers = Map("Authorization" -> "Basic QWxhZGRpbjpPcGVuU2VzYW1l"))

  almaren.builder.sourceSql("""WITH bar AS (select id,name,age,collect_list(phone) as phones from foo group by id,name,age)
    |SELECT concat('http://localhost:3000/xml/',name) as __URL__,id as __ID__, to_json(struct(*)) as __DATA__ FROM bar""".stripMargin)
    .http(method = "GET",session = newSession)
    .deserializer("XML","__BODY__")
    .batch.show(false)

 // test(bigQueryDf, df, "Read bigQuery Test")
  def test(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    testCount(df1, df2, name)
    testCompare(df1, df2, name)
  }
  def testCount(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val count1 = df1.count()
    val count2 = df2.count()
    val count3 = spark.emptyDataFrame.count()
    test(s"Count Test:$name should match") {
      assert(count1 == count2)
    }
    test(s"Count Test:$name should not match") {
      assert(count1 != count3)
    }
  }
  // Doesn't support nested type and we don't need it :)
  def testCompare(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val diff = compare(df1, df2)
    test(s"Compare Test:$name should be zero") {
      assert(diff == 0)
    }
    test(s"Compare Test:$name, should not be able to join") {
      assertThrows[AnalysisException] {
        compare(df2, spark.emptyDataFrame)
      }
    }
  }
  private def compare(df1: DataFrame, df2: DataFrame): Long =
    df1.as("df1").join(df2.as("df2"), joinExpression(df1), "leftanti").count()
  
  private def joinExpression(df1: DataFrame): Column =
    df1.schema.fields
      .map(field => col(s"df1.${field.name}") <=> col(s"df2.${field.name}"))
      .reduce((col1, col2) => col1.and(col2))
  after {
    spark.stop
  }
}
