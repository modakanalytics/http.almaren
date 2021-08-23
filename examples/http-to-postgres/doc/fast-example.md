# How to use almaren.http

Using `almaren.http` to retrieve data from a REST API and send it to a database
is exactly so simple as using the snippet below:

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import org.apache.spark.sql.SaveMode

val almaren = Almaren("HTTP to Postgres Example")

val baseUrl = "https://jsonplaceholder.typicode.com/users/"
val jsonSchema = "..."
val kenyaDsl = "..."

almaren.builder
  .sourceSql("select explode(array(1,2,3,4,5,6,7,8,9,10)) as __ID__")   // Get your existing data
  .sqlExpr("__ID__", s"concat('$baseUrl',__ID__) as __URL__")           // Add one URL for each row
  .http(method = "GET")                                                 // Make the HTTP request
  .deserializer("JSON", "__BODY__", Some(jsonSchema))                   // Convert the payload to JSON
  .dsl(kenyaDsl)                                                        // Use the DSL to flatten data
  .targetJdbc(                                                          // Send data to database
    "jdbc:postgresql://host/dbname",
    "org.postgresql.Driver",
    "dbSchema.dbTable",
    SaveMode.Overwrite,
    Some("user"),
    Some("pass")
  )
  .batch
```

To make an HTTP request for each row on your data, all you need is to provide
a unique id as the column `__ID__` and an URL as the column `__URL__`.

## Getting access to JSON schema

Spark will try to automatically infer the JSON schema of the data returned by
the API.

This is done by batching a second wave of requests to learn about the data.

In order to avoid these duplicated requests you can pass the JSON shcema manually
to `deserializer`.

You can write the schema by yourself if you know how the data will be returned or
let Almaren Framework do this job for you:

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import com.github.music.of.the.ainur.almaren.Util

val almaren = Almaren("HTTP to Postgres Example")

val baseUrl = "https://jsonplaceholder.typicode.com/users/"

val bodyDf = almaren.builder
  .sourceSql("select explode(array(1,2,3,4,5,6,7,8,9,10)) as __ID__")
  .sqlExpr("__ID__", s"concat('$baseUrl',__ID__) as __URL__")
  .http(method = "GET")
  .sql("select __BODY__ from __TABLE__")
  .batch.toDF

val jsonSchema = Util.genDDLFromJsonString(
  df = bodyDf,
  field = "__BODY__",
  sampleRatio = 0.1
)

// jsonSchema: String =
//  `address` STRUCT<
//    `city`: STRING,
//    `geo`: STRUCT<
//      `lat`: STRING,
//      `lng`: STRING
//    >,
//    `street`: STRING,
//    `suite`: STRING,
//    `zipcode`: STRING
//  >,
//  `company` STRUCT<
//    `bs`: STRING,
//    `catchPhrase`: STRING,
//    `name`: STRING
//  >,
//  `email` STRING,
//  `id` BIGINT,
//  `name` STRING,
//  `phone` STRING,
//  `username` STRING,
//  `website` STRING
```

Now you can use this JSON Schema string ou our main snippet.

## Using Kenya DSL to flatten JSON data

Sometimes JSON has nested fields, and you need to flatten them as plain
columns before they can get ingested into a database.

For this task the Kenya DSL makes your work pretty simple, since all you need
is to provide a simple mapping for each field you want in the form:

`nested.json.path` + `dolar sign` + `dest_column_name` + `colon sign` + `DestTypeName`

So let's flatten the nested user data with:

```scala
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
```

So a JSON payload with one user like this:

```json
{
  "id": 1,
  "name": "Leanne Graham",
  "username": "Bret",
  "email": "Sincere@april.biz",
  "address": {
    "street": "Kulas Light",
    "suite": "Apt. 556",
    "city": "Gwenborough",
    "zipcode": "92998-3874",
    "geo": {
      "lat": "-37.3159",
      "lng": "81.1496"
    }
  },
  "phone": "1-770-736-8031 x56442",
  "website": "hildegard.org",
  "company": {
    "name": "Romaguera-Crona",
    "catchPhrase": "Multi-layered client-server neural-net",
    "bs": "harness real-time e-markets"
  }
}
```

Becomes a row on a DataFrame like this:

```
+---+--------------------+-----------------+-----------+-----------+------------------+--------------------+
| id|           full_name|   address_street|address_lat|address_lng|      company_name|  many more columns |
+---+--------------------+-----------------+-----------+-----------+------------------+--------------------+
|  1|       Leanne Graham|      Kulas Light|   -37.3159|    81.1496|   Romaguera-Crona|              ...   |
|  2|        Ervin Howell|    Victor Plains|   -43.9509|   -34.4618|      Deckow-Crist|              ...   |
|...|              ...   |           ...   |     ...   |     ...   |            ...   |              ...   |
| 10|  Clementina DuBuque|  Kattie Turnpike|   -38.2386|    57.2232|        Hoeger LLC|              ...   |
+---+--------------------+-----------------+-----------+-----------+------------------+--------------------+
```
