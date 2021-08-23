package com.modakanalytics

import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import org.apache.spark.sql.SaveMode

object HttpExample {
  def main(args: Array[String]): Unit = {
    val almaren = Almaren("HTTP to Postgres Example")

    val baseUrl = "https://jsonplaceholder.typicode.com/users/"

    val jsonSchema =
      """`address` STRUCT<
        |   `city`: STRING,
        |   `geo`: STRUCT<
        |     `lat`: STRING,
        |     `lng`: STRING
        |   >,
        |   `street`: STRING,
        |   `suite`: STRING,
        |   `zipcode`: STRING
        | >,
        | `company` STRUCT<
        |   `bs`: STRING,
        |   `catchPhrase`: STRING,
        |   `name`: STRING
        | >,
        | `email` STRING,
        | `id` BIGINT,
        | `name` STRING,
        | `phone` STRING,
        | `username` STRING,
        | `website` STRING""".stripMargin

    val kenyaDsl =
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
        | company.bs$company_bs:StringType""".stripMargin

    almaren.builder
      .sourceSql("select explode(array(1,2,3,4,5,6,7,8,9,10)) as __ID__") // Get your existing data
      .sqlExpr("__ID__", s"concat('$baseUrl',__ID__) as __URL__")       // Add one URL for each row
      .http(method = "GET")                                                    // Make the HTTP request
      .deserializer("JSON", "__BODY__", Some(jsonSchema)) // Convert the payload to JSON
      .dsl(kenyaDsl)                                                           // Use the DSL to flatten data
      .targetJdbc(                                                             // Send data to database
        url = s"jdbc:postgresql://localhost/http_to_postgres_database",
        driver = "org.postgresql.Driver", "http_to_postgres_schema.ingested_users",
        saveMode = SaveMode.Overwrite,
        user = Some("mano"),
        password = Some("brau")
      )
      .batch

    sys.exit(0)
  }
}
