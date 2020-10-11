# HTTP Connector

[![Build Status](https://travis-ci.com/modakanalytics/http.almaren.svg?branch=master)](https://travis-ci.com/modakanalytics/http.almaren)

```
libraryDependencies += "com.github.music-of-the-ainur" %% "http-almaren" % "0.0.1-2.4"
```

```
spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.5.0-2.4,com.github.music-of-the-ainur:http-almaren_2.11:0.0.1-2.4"
```

## Input and Output

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

#### Example

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTP.HTTPImplicit
import org.apache.spark.sql.SaveMode

val almaren = Almaren("App Name")

  import spark.implicits._

  val df = Seq(
    ("John", "Smith", "London"),
    ("David", "Jones", "India"),
    ("Michael", "Johnson", "Indonesia"),
    ("Chris", "Lee", "Brazil"),
    ("Mike", "Brown", "Russia")
  ).toDF("first_name", "last_name", "country")

  df.createOrReplaceTempView("person_info")

val schema = "`data` STRUCT<`age`: BIGINT, `country`: STRING, `full_name`: STRING, `salary`: BIGINT>"

val df = almaren.builder
   .sourceSql("""with cte as (select monotonically_increasing_id() as id,* from person_info)
                 |SELECT concat('http://localhost:3000/fireshots/',first_name,last_name,'/',country) as __URL__,id as __ID__, to_json(struct(first_name,last_name,country)) as __DATA__ FROM cte""".stripMargin).alias("PERSON_DATA")
    .http(method = "GET")
    .deserializer("JSON","__BODY__",Some(schema))
    .sql("select __ID__,data,__STATUS_CODE__ as status_code,__ELAPSED_TIME__ as elapsed_time from __TABLE__")
    .dsl(
      """__ID__$__ID__:StringType
        |elapsed_time$elapsed_time:LongType
        |data.full_name$full_name:StringType
        |data.country$country:StringType
        |data.age$age:LongType
        |data.salary$salary:DoubleType
        |status_code$status_code:IntegerType""".stripMargin
    )
    .sql(
      """select T.__ID__ as id ,
         full_name ,
         country
         age,
         salary,
         status_code
        from __TABLE__ T join PERSON_DATA P on T.__ID__ = P.__ID__""")
    .batch

df.show(false)

```

