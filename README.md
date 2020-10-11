# HTTP Connector

[![Build Status](https://travis-ci.com/modakanalytics/http.almaren.svg?branch=master)](https://travis-ci.com/modakanalytics/http.almaren)

```
libraryDependencies += "com.github.music-of-the-ainur" %% "http-almaren" % "0.0.1-2.4"
```

```
spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.5.0-2.4,com.github.music-of-the-ainur:http-almaren_2.11:0.0.1-2.4"
```

## Example

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTP.HTTPImplicit

import spark.implicits._

// Generating table "USER_DATA"

List(Data("Roger","Laura",25,2342324232L),Data("Robert","Dickson",88,3218313131L),Data("Daniel","Pedro",28,32323232L)).toDS.createOrReplaceTempView("USER_DATA")


// Don't infer the schema manually, just follow the steps:
// val jsonColumn = spark.sql("SELECT JSON_STRING_COL FROM ...").as[String]
// To generate the schema: spark.read.json(jsonColumn).schema.toDDL

val httpOutpustSchema = Some("`data` STRING,`headers` STRUCT<`Accept`: STRING, `Accept-Encoding`: STRING, `Cache-Control`: STRING, `Content-Length`: STRING, `Content-Type`: STRING, `Host`: STRING, `Pragma`: STRING, `User-Agent`: STRING, `X-Amzn-Trace-Id`: STRING>,`method` STRING,`origin` STRING,`url` STRING")


val df = almaren.builder
    .sourceSql("SELECT uuid() as id,* FROM USER_DATA").alias("DATA")
    .sql("""SELECT 
                id as __ID__,
                concat('http://httpbin.org/anything/person/',code) as __URL__,
                to_json(named_struct('data',named_struct('name',firstName + " " + lastName))) as __DATA__ 
            FROM DATA""")
    .http(method = "POST")
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
20/10/11 10:28:11 INFO SourceSql: sql:{SELECT uuid() as id,* FROM USER_DATA}
20/10/11 10:28:11 INFO Alias: {DATA}
20/10/11 10:28:11 INFO Sql: sql:{SELECT
                id as __ID__,
                concat('http://httpbin.org/anything/person/',code) as __URL__,
                to_json(named_struct('data',named_struct('name',firstName + " " + lastName))) as __DATA__
            FROM DATA}
20/10/11 10:28:11 INFO MainHTTP: headers:{Map()}, method:{POST}
20/10/11 10:28:11 INFO JsonDeserializer: columnName:{__BODY__}, schema:{Some(`data` STRING,`headers` STRUCT<`Accept`: STRING, `Accept-Encoding`: STRING, `Cache-Control`: STRING, `Content-Length`: STRING, `Content-Type`: STRING, `Host`: STRING, `Pragma`: STRING, `User-Agent`: STRING, `X-Amzn-Trace-Id`: STRING>,`method` STRING,`origin` STRING,`url` STRING)}
20/10/11 10:28:11 INFO Sql: sql:{SELECT
                T.origin,
                D.firstName,
                D.lastName,D.age,
                T.__STATUS_CODE__ as status_code,
                T.url,
                T.__ERROR__ as error,
                T.__ELAPSED_TIME__ as request_time
            FROM __TABLE__ T JOIN DATA D ON d.id = t.__ID__}
            
+-------------+---------+--------+---+-----------+---------------------------------------------+-----+------------+
|origin       |firstName|lastName|age|status_code|url                                          |error|request_time|
+-------------+---------+--------+---+-----------+---------------------------------------------+-----+------------+
|151.68.13.200|Roger    |Laura   |25 |200        |http://httpbin.org/anything/person/2342324232|null |421         |
|151.68.13.200|Robert   |Dickson |88 |200        |http://httpbin.org/anything/person/3218313131|null |409         |
|151.68.13.200|Daniel   |Pedro   |28 |200        |http://httpbin.org/anything/person/32323232  |null |409         |
+-------------+---------+--------+---+-----------+---------------------------------------------+-----+------------+
```

## Input and Output

_http_ component parameters:

| Parameter      | Description                     | Type                                                               |
|----------------|---------------------------------|--------------------------------------------------------------------|
| headers        | HTTP headers                    | Map[String,String]                                                 |
| method         | HTTP Method                     | String                                                             |
| requestHandler | Closure to handle HTTP request  | (Row,Session,String,Map[String,String],String) => requests.Respons |
| session        | Closure to handle HTTP sessions | () = requests.Session                                              |


*HTTP Component* special _columns_: 

### Input :
#### Parameters

| Parameters   | Mandatory | Description                                                                        |
|--------------|-----------|------------------------------------------------------------------------------------|
| \_\_ID\_\_   | Yes       | This field will be in response of http.almaren component, it's useful to join data |
| \_\_URL\_\_  | Yes       | Used to perform the HTTP request                                                   |
| \_\_DATA\_\_ | No        | Data Content (productName,producePrice) , used in POST Method HTTP requests        |


### Output:
#### Parameters

| Parameters           | Description                                        |
|----------------------|----------------------------------------------------|
| \_\_ID\_\_       | Custom ID , This field will be useful to join data |
| \_\_BODY\_\_         | HTTP response                                      |
| \_\_HEADER\_\_       | HTTP header                                        |
| \_\_STATUS_CODE\_\_  | HTTP response code                                 |
| \_\_STATUS_MSG\_\_   | HTTP response message                              |
| \_\_ERROR\_\_        | Java Exception                                     |
| \_\_ELAPSED_TIME\_\_ | Request time in ms                                 |

## Methods

The following methods are supported:

- POST
- GET
- HEAD
- OPTIONS
- DELETE
- PUT

## Session

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

## Request Handler

You can overwrite the default _requestHandler_ closure to give any custom HTTP Request.

```scala

val customHandler = (row:Row,session:Session,url:String, headers:Map[String,String], method:String) => {
    method.toUpperCase match {
      case "GET" => session.get(url, params = headers)
      case "DELETE" => session.delete(url, params = headers)
      case "OPTIONS" => session.options(url, params = headers)
      case "HEAD" => session.head(url, params = headers)
      case "POST" => session.post(url, params = headers, data = row.getAs[String](Alias.DataCol))
      case "PUT" => session.put(url, params = headers, data = row.getAs[String](Alias.DataCol))
      case method => throw new Exception(s"Invalid Method: $method")
     }
     
almaren.builder
    .sql("...")
    .http(method = "POST", requestHandler = customHandler)
```

