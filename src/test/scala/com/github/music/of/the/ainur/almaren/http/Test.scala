package com.github.music.of.the.ainur.almaren.http

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SaveMode, SparkSession}
import org.scalatest._
import org.apache.spark.sql.functions._
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

class Test extends AnyFunSuite with BeforeAndAfter {
  val almaren = Almaren("http-almaren")

  val spark: SparkSession = almaren.spark
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("com.github.music.of.the.ainur.almaren").setLevel(Level.INFO)

  import spark.implicits._

  val df = Seq(
    ("John", "Smith", "London"),
    ("David", "Jones", "India"),
    ("Michael", "Johnson", "Indonesia"),
    ("Chris", "Lee", "Brazil"),
    ("Mike", "Brown", "Russia")
  ).toDF("first_name", "last_name", "country").coalesce(1)

  df.createOrReplaceTempView("person_info")

  val newSession = () => requests.Session(headers = Map("Authorization" -> "Basic QWxhZGRpbjpPcGVuU2VzYW1l"))

  val schema = "`data` STRUCT<`age`: BIGINT, `country`: STRING, `full_name`: STRING, `salary`: BIGINT>"

  val queryGet =
    """with cte as (select monotonically_increasing_id() as id,* from person_info)
      |SELECT concat('http://localhost:3000/fireshots/',first_name,last_name,'/',country) as __URL__,id as __ID__, to_json(struct(first_name,last_name,country)) as __DATA__ FROM cte""".stripMargin

  val queryPost =
    """with cte as (select monotonically_increasing_id() as id,* from person_info)
      |SELECT 'http://localhost:3000/fireshots/getInfo' as __URL__,id as __ID__, to_json(struct(first_name,last_name,country)) as __DATA__ FROM cte""".stripMargin


  val postSessionDf = spark.read.parquet("src/test/resources/data/postSession.parquet")
  val postDf = spark.read.parquet("src/test/resources/data/postWithoutSession.parquet")
  val getSessionDf = spark.read.parquet("src/test/resources/data/getSession.parquet")
  val getDf = spark.read.parquet("src/test/resources/data/getWithoutSession.parquet")

  test(postSessionDf, getHttpDf(queryPost, "POST", true), "POST with Session")
  test(postDf, getHttpDf(queryPost, "POST", false), "POST without Session")
  test(getSessionDf, getHttpDf(queryGet, "GET", true), "GET with Session")
  test(getDf, getHttpDf(queryGet, "GET", false), "GET without Session")


  def getHttpDf(query: String, methodType: String, isSession: Boolean): DataFrame = {

    val tempDf = if (isSession) {
      almaren.builder
        .sourceSql(query).alias("PERSON_DATA")
        .http(params = Map("username" -> "sample"), hiddenParams = Map("username" -> "sample", "password" -> "sample"), method = methodType, session = newSession)
    }
    else {
      almaren.builder
        .sourceSql(query).alias("PERSON_DATA")
        .http(method = methodType)
    }

    tempDf
      .deserializer("JSON", "__BODY__", Some(schema)).alias("TABLE")
      .sql("select __ID__,data,__STATUS_CODE__ as status_code,__ELAPSED_TIME__ as elapsed_time from TABLE").alias("TABLE1")
      .dsl(
        """__ID__$__ID__:StringType
          |elapsed_time$elapsed_time:LongType
          |data.full_name$full_name:StringType
          |data.country$country:StringType
          |data.age$age:LongType
          |data.salary$salary:DoubleType
          |status_code$status_code:IntegerType""".stripMargin
      ).alias("TABLE2")
      .sql(
        """select T.__ID__ as id ,
         full_name ,
         country
         age,
         salary,
         status_code
        from TABLE2 T join PERSON_DATA P on T.__ID__ = P.__ID__""")
      .batch
  }

  val httpBatchDf = almaren.builder
    .sourceDataFrame(df)
    .sqlExpr("to_json(struct(*)) as __DATA__", "monotonically_increasing_id() as __ID__").alias("BATCH_DATA")
    .httpBatch(
      url = "http://127.0.0.1:3000/batchAPI",
      params = Map("username" -> "sample"),
      hiddenParams = Map("username" -> "sample", "password" -> "sample"),
      method = "POST",
      batchSize = 3,
      batchDelimiter = (rows: Seq[Row]) => s"""[${rows.map(row => row.getAs[String](Alias.DataCol)).mkString(",")}]""")
    .deserializer("JSON", "__BODY__", Some("`data` ARRAY<STRUCT<`country`: STRING, `first_name`: STRING, `last_name`: STRING>>")).alias("TABLE")
    .sql("select   explode(arrays_zip(__ID__, data)) as vars , __STATUS_CODE__ as status_code,__ELAPSED_TIME__ as elapsed_time from TABLE").alias("TABLE1")
    .sql("select vars.__ID__ as __ID__ ,vars.data as data ,status_code, elapsed_time from TABLE1 ").alias("TABLE2")
    .dsl(
      """__ID__$__ID__:StringType
        |data.country$country:StringType
        |data.first_name$first_name:StringType
        |data.last_name$last_name:StringType
        |status_code$status_code:IntegerType
        |elapsed_time$elapsed_time:LongType""".stripMargin).alias("TABLE3")
    .sql("select TABLE3.__ID__ as id ,first_name,last_name,country,status_code  from TABLE3 inner join BATCH_DATA on TABLE3.__ID__ = BATCH_DATA.__ID__ ")
    .batch

  val getBatchDf = spark.read.parquet("src/test/resources/data/httpBatch.parquet")

  test(getBatchDf, httpBatchDf, "test for httpBatch method")


  val requestSchema = StructType(Seq(
    StructField("__URL__", StringType),
    StructField("__DATA__", StringType),
    StructField("__REQUEST_HEADERS__", MapType(StringType, StringType)),
    StructField("__REQUEST_PARAMS__", MapType(StringType, StringType)),
    StructField("__REQUEST_HIDDEN_PARAMS__", MapType(StringType, StringType))
  ))

  val requestRows: Seq[Row] = df.toLocalIterator.asScala.toList.map(row => {
    val firstName = row.getAs[String]("first_name")
    val lastName = row.getAs[String]("last_name")
    val country = row.getAs[String]("country")
    val url = s"http://localhost:3000/fireshots/getInfo"
    val headers = scala.collection.mutable.Map[String, String]()
    headers.put("data", firstName)
    val params = scala.collection.mutable.Map[String, String]()
    params.put("params", lastName)
    val hiddenParams = scala.collection.mutable.Map[String, String]()
    hiddenParams.put("hidden_params", country)

    Row(url,
      s"""{"first_name" : "$firstName","last_name":"$lastName","country":"$country"} """,
      headers,
      params,
      hiddenParams
    )
  })

  val requestDataframe = spark.createDataFrame(spark.sparkContext.parallelize(requestRows), requestSchema)

  val finalRequestDataframe = requestDataframe.selectExpr("monotonically_increasing_id() as __ID__", "*")

  val postSessionRowDf = spark.read.parquet("src/test/resources/data/postRowSession.parquet")
  val postRowDf = spark.read.parquet("src/test/resources/data/postRowWithoutSession.parquet")

  test(postSessionRowDf, getHttpRowDf(finalRequestDataframe, "POST", isSession = true), "POST with Headers and Params from ROW with Session")
  test(postRowDf, getHttpRowDf(finalRequestDataframe, "POST", isSession = false), "POST with Headers and Params from ROW without Session")

  def getHttpRowDf(df: DataFrame, methodType: String, isSession: Boolean): DataFrame = {

    val tempDf = if (isSession) {
      almaren.builder
        .sourceDataFrame(df).alias("REQUEST_DATA")
        .sqlExpr("__ID__", "__DATA__", "__URL__", "__REQUEST_HEADERS__", "__REQUEST_PARAMS__", "__REQUEST_HIDDEN_PARAMS__")
        .http(method = methodType, session = newSession, params = Map("username" -> "sample"), hiddenParams = Map("username" -> "sample", "password" -> "sample"))
    }
    else {
      almaren.builder
        .sourceDataFrame(df).alias("REQUEST_DATA")
        .sqlExpr("__ID__", "__DATA__", "__URL__", "__REQUEST_HEADERS__", "__REQUEST_PARAMS__", "__REQUEST_HIDDEN_PARAMS__")
        .http(method = methodType, params = Map("username" -> "sample"), hiddenParams = Map("username" -> "sample", "password" -> "sample"))
    }

    tempDf
      .deserializer("JSON", "__BODY__", Some(schema)).alias("RESPONSE_DATA")
      .sql(
        """select data,
          | b.__URL__ as http_request_url ,
          | b.__DATA__ as http_payload,
          | cast(a.__REQUEST_HEADERS__ as STRING) as http_request_headers,
          | cast(a.__REQUEST_PARAMS__ as STRING) as http_request_params,
          | cast(a.__REQUEST_HIDDEN_PARAMS__ as STRING) as http_request__hidden_params,
          | __STATUS_CODE__ as status_code from REQUEST_DATA a INNER JOIN RESPONSE_DATA b on a.__ID__=b.__ID__""".stripMargin)
      .batch
  }

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
