# HTTP Connector

[![Build Status](https://travis-ci.com/modakanalytics/http.almaren.svg?token=TEB3zRDqVUuChez9334q&branch=master)](https://travis-ci.com/modakanalytics/http.almaren)
```
libraryDependencies += "com.github.music-of-the-ainur" %% "http-almaren" % "1.0.1-2.4"
```

```
spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.9.0-2.4,com.github.music-of-the-ainur:http-almaren_2.11:1.0.1-2.4"
```
## Table of Contents

- [Methods](#methods)
  * [HTTP](#http)
    + [Example](#example)
    + [Parameters](#parameters)
    + [Special Columns](#special-columns)
      - [Input:](#input-)
      - [Output:](#output-)
    + [Methods](#methods-1)
    + [Session](#session)
    + [Request Handler](#request-handler)
  * [HTTP Batch](#http-batch)
    + [Example](#example-1)
    + [Parameters](#parameters-1)
    + [Special Columns](#special-columns-1)
      - [Input:](#input--1)
      - [Output:](#output--1)
    + [Methods](#methods-2)
    + [Request Handler Batch](#request-handler-batch)
      - [Batch Delimiter](#batch-delimiter)
      - [Examples](#examples)

## Methods

### HTTP

It will perform a HTTP request for each `Row`.

```
$ curl -X PUT -H "Authorization: {SESSION_ID}" \
-H "Content-Type: application/x-www-form-urlencoded" \
-d "language=English" \
-d "product=32131314" \
-d "audience_=consumer_vr" \
https://localhost/objects/documents/534
```

#### Example

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit

import spark.implicits._

// Generating table "USER_DATA"

case class Data(firstName:String, lastName:String, age:Int, code:Long)

List(Data("Roger","Laura",25,2342324232L),
    Data("Robert","Dickson",88,3218313131L),
    Data("Daniel","Pedro",28,32323232L))
    .toDS
    .createOrReplaceTempView("USER_DATA")


// Don't infer the schema manually, just follow the steps:
// val jsonColumn = spark.sql("SELECT __BODY__ FROM ...").as[String]
// To generate the schema: spark.read.json(jsonColumn).schema.toDDL

val httpOutpustSchema = Some("`data` STRING,`headers` STRUCT<`Accept`: STRING, `Accept-Encoding`: STRING, `Cache-Control`: STRING, `Content-Length`: STRING, `Content-Type`: STRING, `Host`: STRING, `Pragma`: STRING, `User-Agent`: STRING, `X-Amzn-Trace-Id`: STRING>,`method` STRING,`origin` STRING,`url` STRING")


val df = almaren.builder
    .sourceSql("SELECT uuid() as id,* FROM USER_DATA").alias("DATA")
    .sql("""SELECT 
                id as __ID__,
                concat('http://httpbin.org/anything/person/',code) as __URL__,
                to_json(named_struct('data',named_struct('name',firstName + " " + lastName))) as __DATA__ 
            FROM DATA""")
    .http(method = "POST", threadPoolSize = 10, batchSize = 10000)
    .deserializer("JSON","__BODY__",httpOutpustSchema)
    .sql("""SELECT
                T.origin,
                D.firstName,
                D.lastName,D.age,
                T.__STATUS_CODE__ as status_code,
                T.url,
                T.__ERROR__ as error,
                T.__ELAPSED_TIME__ as request_time
            FROM __TABLE__ T JOIN DATA D ON d.id = t.__ID__""")
    .batch

df.show(false)
```

Output:
```
20/10/15 01:04:32 INFO SourceSql: sql:{SELECT uuid() as id,* FROM USER_DATA}
20/10/15 01:04:33 INFO Alias: {DATA}
20/10/15 01:04:33 INFO Sql: sql:{SELECT
                id as __ID__,
                concat('http://httpbin.org/anything/person/',code) as __URL__,
                to_json(named_struct('data',named_struct('name',firstName + " " + lastName))) as __DATA__
            FROM DATA}
20/10/15 01:04:33 INFO MainHTTP: headers:{Map()}, method:{POST}, connectTimeout:{60000}, readTimeout{1000}, threadPoolSize:{10}
20/10/15 01:04:33 INFO JsonDeserializer: columnName:{__BODY__}, schema:{Some(`data` STRING,`headers` STRUCT<`Accept`: STRING, `Accept-Encoding`: STRING, `Cache-Control`: STRING, `Content-Length`: STRING, `Content-Type`: STRING, `Host`: STRING, `Pragma`: STRING, `User-Agent`: STRING, `X-Amzn-Trace-Id`: STRING>,`method` STRING,`origin` STRING,`url` STRING)}
20/10/15 01:04:33 INFO Sql: sql:{SELECT
                T.origin,
                D.firstName,
                D.lastName,D.age,
                T.__STATUS_CODE__ as status_code,
                T.url,
                T.__ERROR__ as error,
                T.__ELAPSED_TIME__ as request_time
            FROM __TABLE__ T JOIN DATA D ON d.id = t.__ID__}
            
+-----------+---------+--------+---+-----------+---------------------------------------------+-----+------------+
|origin     |firstName|lastName|age|status_code|url                                          |error|request_time|
+-----------+---------+--------+---+-----------+---------------------------------------------+-----+------------+
|151.46.49.9|Roger    |Laura   |25 |200        |http://httpbin.org/anything/person/2342324232|null |651         |
|151.46.49.9|Robert   |Dickson |88 |200        |http://httpbin.org/anything/person/3218313131|null |653         |
|151.46.49.9|Daniel   |Pedro   |28 |200        |http://httpbin.org/anything/person/32323232  |null |543         |
+-----------+---------+--------+---+-----------+---------------------------------------------+-----+------------+
```

#### Parameters

| Parameter      | Description                                                                                                             | Type                                                               |
|----------------|-------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| headers        | HTTP headers                                                                                                            | Map[String,String]                                                 |
| params         | HTTP params                                                                                                             | Map[String,String]                                                 |
| method         | HTTP Method                                                                                                             | String                                                             |
| requestHandler | Closure to handle HTTP request                                                                                          | (Row,Session,String,Map[String,String],String) => requests.Respons |
| session        | Closure to handle HTTP sessions                                                                                         | () = requests.Session                                              |
| connectTimeout | Timeout in ms to keep the connection keep-alive, it's recommended to keep this number high                              | Int                                                                |
| readTimeout    | Maximum number of ms to perform a single HTTP request                                                                   | Int                                                                |
| threadPoolSize | How many connections in parallel for each executor. parallelism = number of excutors * number of cores * threadPoolSize | Int                                                                |
| batchSize      | How many records a single thread will process                                                                           | Int                                                                |


#### Special Columns

##### Input:

| Parameters   | Mandatory | Description                                                                        |
|--------------|-----------|------------------------------------------------------------------------------------|
| \_\_ID\_\_   | Yes       | This field will be in response of http.almaren component, it's useful to join data |
| \_\_URL\_\_  | Yes       | Used to perform the HTTP request                                                   |
| \_\_DATA\_\_ | No        | Data Content (productName,producePrice) , used in POST/PUT Method HTTP requests    |


##### Output:

| Parameters           | Description                                        |
|----------------------|----------------------------------------------------|
| \_\_ID\_\_       | Custom ID , This field will be useful to join data |
| \_\_BODY\_\_         | HTTP response                                      |
| \_\_HEADER\_\_       | HTTP header                                        |
| \_\_STATUS_CODE\_\_  | HTTP response code                                 |
| \_\_STATUS_MSG\_\_   | HTTP response message                              |
| \_\_ERROR\_\_        | Java Exception                                     |
| \_\_ELAPSED_TIME\_\_ | Request time in ms                                 |

#### Methods

The following methods are supported:

- POST
- GET
- HEAD
- OPTIONS
- DELETE
- PUT

#### Session

You can give an existing [session](https://github.com/lihaoyi/requests-scala#sessions) to the HTTP component.
To see all details check the [documentation](https://github.com/lihaoyi/requests-scala#sessions)


```scala
val newSession = () => {
    val s = requests.Session(headers = Map("Custom-header" -> "foo"))
    s.post("https://bar.com/login",data = Map("user" -> "baz", "password" -> "123"))
    s
}

almaren.builder
    .sourceSql("SELECT concat('http://localhost:3000/user/',first_name,last_name,'/',country) as __URL__,id as __ID__")
    .http(method = "GET", session = newSession)

```

#### Request Handler

You can overwrite the default _requestHandler_ closure to give any custom HTTP Request.

```scala

val customHandler = (row:Row,session:Session,url:String, headers:Map[String,String], method:String,connectTimeout:Int, readTimeout:Int) => {
    method.toUpperCase match {
      case "GET" => session.get(url, params = headers, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "DELETE" => session.delete(url, params = headers, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "OPTIONS" => session.options(url, params = headers, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "HEAD" => session.head(url, params = headers, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "POST" => session.post(url, params = headers, data = row.getAs[String](Alias.DataCol), readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "PUT" => session.put(url, params = headers, data = row.getAs[String](Alias.DataCol), readTimeout = readTimeout, connectTimeout = connectTimeout)
      case method => throw new Exception(s"Invalid Method: $method")
    }
}
     
almaren.builder
    .sql("...")
    .http(method = "POST", requestHandler = customHandler)
```

### HTTP Batch

Is used to perform a single HTTP request with a batch of data. You can choose how to create the batch data using the `batchDelimiter` closure. 
The default behavior is to concatenate by new line.

```
$ curl -X PUT -H "Authorization: {SESSION_ID}" \
-H "Content-Type: text/csv" \
-H "Accept: text/csv" \
--data-binary @"filename" \
https://localhost/objects/documents/batch
```

#### Example 

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit

import spark.implicits._

  val httpBatchDf = almaren.builder
    .sourceDataFrame(df)
    .sqlExpr("to_json(struct(*)) as __DATA__", "monotonically_increasing_id() as __ID__").alias("BATCH_DATA")
    .httpBatch(
      url = "http://127.0.0.1:3000/batchAPI",
      method = "POST",
      batchSize = 3,
      batchDelimiter = (rows: Seq[Row]) => s"""[${rows.map(row => row.getAs[String](Alias.DataCol)).mkString(",")}]""")
    .batch

httpBatchDf.show(false)
```

Output:
```
21/06/09 17:52:20 INFO SourceDataFrame:
21/06/09 17:52:20 INFO SqlExpr: exprs:{to_json(struct(*)) as __DATA__
monotonically_increasing_id() as __ID__}
21/06/09 17:52:20 INFO Alias: {BATCH_DATA}
21/06/09 17:52:20 INFO HTTPBatch: url:{http://127.0.0.1:3000/batchAPI}, headers:{Map()},params:{Map()}, method:{POST}, connectTimeout:{60000}, readTimeout{1000}, batchSize:{3}com.github.music.of.the.ainur.almaren.http.Test 93s
+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+---------------+--------------+---------+----------------+------------------------------+
|__ID__   |__BODY__                                                                                                                                                                                                 |__HEADER__                                                                                                                                          |__STATUS_CODE__|__STATUS_MSG__|__ERROR__|__ELAPSED_TIME__|__URL__                       |
+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+---------------+--------------+---------+----------------+------------------------------+
|[0, 1, 2]|{"data":[{"country":"London","first_name":"John","last_name":"Smith"},{"country":"India","first_name":"David","last_name":"Jones"},{"country":"Indonesia","first_name":"Michael","last_name":"Johnson"}]}|[date -> [Wed, 09 Jun 2021 12:22:23 GMT], content-type -> [application/json;charset=UTF-8], server -> [Mojolicious (Perl)], content-length -> [201]]|200            |OK            |null     |122             |http://127.0.0.1:3000/batchAPI|
|[3, 4]   |{"data":[{"country":"Brazil","first_name":"Chris","last_name":"Lee"},{"country":"Russia","first_name":"Mike","last_name":"Brown"}]}                                                                      |[date -> [Wed, 09 Jun 2021 12:22:23 GMT], content-type -> [application/json;charset=UTF-8], server -> [Mojolicious (Perl)], content-length -> [131]]|200            |OK            |null     |171             |http://127.0.0.1:3000/batchAPI|
+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+---------------+--------------+---------+----------------+------------------------------+```
``` 

#### Parameters

| Parameter      | Description                                                                                | Type                                                               |
|----------------|--------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| url            | Used to perform the HTTP request                                                           | String                                                             |
| headers        | HTTP headers                                                                               | Map[String,String]                                                 |
| params         | HTTP params                                                                                | Map[String,String]                                                 |
| method         | HTTP Method                                                                                | String                                                             |
| requestHandler | Closure to handle HTTP request                                                             | (Row,Session,String,Map[String,String],String) => requests.Respons |
| session        | Closure to handle HTTP sessions                                                            | () = requests.Session                                              |
| connectTimeout | Timeout in ms to keep the connection keep-alive, it's recommended to keep this number high | Int                                                                |
| readTimeout    | Maximum number of ms to perform a single HTTP request                                      | Int                                                                |
| batchSize      | Number of records sent in a single HTTP transaction                                        | Int                                                                |
| batchDelimiter | Closure used to determine how the batch data will be created                               | (Seq[Row]) => String                                               |


#### Special Columns

##### Input:

| Parameters   | Mandatory | Description                                                                                                |
|--------------|-----------|------------------------------------------------------------------------------------------------------------|
| \_\_ID\_\_   | Yes       | This field will be in response of http.almaren component which is array[string] , it's useful to join data |
| \_\_DATA\_\_ | No        | Data Content (productName,producePrice) , used in POST/PUT Method HTTP requests                            |


##### Output:

| Parameters           | Description                                                                       |
|----------------------|-----------------------------------------------------------------------------------|
| \_\_ID\_\_           | Custom ID , This field will be useful to join data which is of type array[string] |
| \_\_BODY\_\_         | HTTP response                                                                     |
| \_\_HEADER\_\_       | HTTP header                                                                       |
| \_\_STATUS_CODE\_\_  | HTTP response code                                                                |
| \_\_STATUS_MSG\_\_   | HTTP response message                                                             |
| \_\_ERROR\_\_        | Java Exception                                                                    |
| \_\_ELAPSED_TIME\_\_ | Request time in ms                                                                |

#### Methods

The following methods are supported:

- POST
- GET
- HEAD
- OPTIONS
- DELETE
- PUT

#### Request Handler Batch

You can overwrite the default `_requestHandlerBatch_` closure to give any custom HTTP Request.

```scala

  val customHandlerBatch = (data:String, session:Session, url:String, headers:Map[String, String], params:Map[String, String], method:String, connectTimeout:Int, readTimeout:Int) => {
    method.toUpperCase match {
      case "GET" => session.get(url, headers = headers, params = params, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "DELETE" => session.delete(url, headers = headers, params = params, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "OPTIONS" => session.options(url, headers = headers, params = params, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "HEAD" => session.head(url, headers = headers, params = params, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "POST" => session.post(url, headers = headers, params = params, data = data, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "PUT" => session.put(url, headers = headers, params = params, data = data, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case method => throw new Exception(s"Invalid Method: $method")
    }
  }
     
almaren.builder
    .sql("...")
    .httpBatch(method = "POST", requestHandler = customHandlerBatch)
```

##### Batch Delimiter

Is a closure used to determine how the batch data will be created. The default behavior is to concatenate by new line.
Example, if your `__DATA__` column has string by row. It will create a batch where the number of lines is defined by the `batchSize` parameter:

```
foo
bar
baz
...
```

##### Examples 

If the `__DATA__` column is a JSON string `{foo:"bar"}` where you need to convert to an array of JSON `[{foo:"bar"},{foo:"baz"}}`:

```scala
(rows:Seq[Row]) => s"""[${rows.map(row => row.getAs[String](Alias.DataCol)).mkString(",")}]""")
```

How to concatenate by new line:

```scala
(rows:Seq[Row]) => rows.map(row => row.getAs[String](Alias.DataCol)).mkString("\n")
```
