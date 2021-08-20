package com.modakanalytics

import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import org.apache.spark.sql.SaveMode

object HttpExample {
  def main(args: Array[String]): Unit = {
    val almaren = Almaren("HTTP to Postgres Example")

    val spark = almaren.spark
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    val sourceDf = almaren.builder
      .sourceDataFrame(spark.range(1, 11))
      .alias("SOURCE_DATA")

    val baseUrl = "https://jsonplaceholder.typicode.com/users/"
    val payload = sourceDf
      .sql(s"select id as __ID__, concat('$baseUrl', id) as __URL__ from SOURCE_DATA")
      .http(method = "GET")
      .deserializer("JSON", "__BODY__")

    val users = payload
      .dsl(
        """
          | id$id:LongType
          | name$full_name:StringType
          | username$username:StringType
          | email$email:StringType
          | phone$phone:StringType
          | website$site:StringType
          | address.street$address_street:StringType
          | address.suite$address_suite:StringType
          | address.city$address_city:StringType
          | address.zipcode$address_zipcode:StringType
          | address.geo.lat$address_lat:StringType
          | address.geo.lng$address_lng:StringType
          | company.name$company_name:StringType
          | company.catchPhrase$company_catch_phrase:StringType
          | company.bs$company_bs:StringType""".stripMargin)

    val dbHost = "localhost"
    val dbPort = 5432
    val dbUser = "mano"
    val dbPass = "brau"
    val dbName = "http_to_postgres_database"
    val dbSchema = "http_to_postgres_schema"
    val targetTable = "ingested_users"

    users
      .targetJdbc(
        s"jdbc:postgresql://$dbHost:$dbPort/$dbName",
        "org.postgresql.Driver",
        s"$dbSchema.$targetTable",
        SaveMode.Overwrite,
        Some(dbUser),
        Some(dbPass)
      )
      .batch
      .show()

    sys.exit(0)
  }
}
