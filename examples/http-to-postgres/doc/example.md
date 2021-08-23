# Consuming a REST API to ingest data into a Postgres database

In this example we are going to show you how to use the HTTP connector to
consume a REST API and ingest this data into a Postgres database.

We will:

1. Build and run a application;
2. Consume user data from the [JSON Placeholder API](https://jsonplaceholder.typicode.com/users/);
3. Flatten nested fields to a plain Data Frame using [Kenya DSL](https://github.com/modakanalytics/quenya-dsl);
4. Ingest collected data into a local Postgres running at a local container.

## Requirements

In order to correct run this example you will need:

1. Internet access to be able to connect to the
[JSON Placeholder API](https://jsonplaceholder.typicode.com/users/1).
2. `docker` (any version) and `docker-compose` (1.29.x later).
3. `JDK 8` (OpenJDK 1.8.x recommended)
4. `spark 2.4.8` (exactly)
5. `scala 2.11.12` (exactly)
6. `sbt` (any version)

The `docker` and `docker-compose` commands will be used to run a Postgres
container locally.

JDK, spark and scala will be used to write and run the application.

`sbt` will be used to generate the jar file of the application.

## Preparing the Postgres Server

This section is optional. You can skip it if you already have a postgres
database properly configured and running.

Keep in mind that we just want:

* A Postgres `server` listening on a `port` at a `host`;
* A `database` on that server;
* A `schema` on that `database`;
* Credentials (user and password) to access the `server`, `database` and `schema`.

All we need to run a Postgres container is the pair of files `.env` and
`docker-compose.yml`.

The `.env` file is pretty self-explanatory. Take note of the passwords for the
privileged user `postgres` and the non-privileged user `mano`.

The `docker-compose.yml` will define some stuff to provide a container with
a postgres server and convenient access to a `psql` terminal.

### Running the Postgres server

To run the postgres server on background, open a terminal on the root directory
of this example and type:

```
user@host$ docker-compose up -d db
```

### Creating the user and database

To create the user and the database we need to log into Postgres server as a
privileged user. To do this, just type on the same terminal:

```
user@host$ PG_USER=postgres PG_DB=template1 docker-compose run psql
```

You will be asked to enter the `postgres` password "postgres".

On the psql prompt just execute the database creation file:

```
template1=# \i /sql/00-init_database.sql
```

Just type `Ctrl+D` to quit from psql and go back to the terminal.

### Creating the database schema

To create the schema inside the example database, let's use `psql` again, now
as the unprivileged user `mano`. All defaults are properly set on `.env` file.
Just type on terminal:

```
user@host$ docker-compose run psql
```

You will be asked to enter the `mano` password "brau".

On the psql prompt just execute the schema creation file:

```
http_to_postgres_database=> \i /sql/01-create-schema.sql
```

You can list all tables inside the schema by typing `\dt`
but we didn't create any table yet.

## Running the example using the spark-shell

In this section we will show how to run the example code step by step using
the interactive `spark-shell`. You can skip it if you are comfortable writing
and running spark applications.

Open a new terminal and go to the root directory of this example.

You can run a spark shell by typing the command:

```
user@host$ spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.9.0-2.4,com.github.music-of-the-ainur:http-almaren_2.11:1.2.0-2.4,org.postgresql:postgresql:42.2.23"
```

Note that we need to pass as arguments the packages for the Almaren Framework
and the postgres JDBC driver.

After some initialization we get a scala prompt:

```
scala> 
```

This pipeline is pretty simple and could be expressed shortly as showed on the
final code by just chaining each method with the following one, but we are
going to split them into separated steps for better explaining them.

We are also omitting the line breaks, so you can just copy-paste the commands
line by line.

To start, let's import all packages we will use and get the `almaren` object:

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import org.apache.spark.sql.SaveMode

val almaren = Almaren("HTTP to Postgres Example")
```

The HTTP connector is meant to make one request for each row of a given data
frame. In a real world application you will get it from a file or another
database but here we will just create a fake data source.

The important thing to note is that HTTP connector expects to see at least
two columns, an ID called `__ID__` and an url called `__URL__`.

In our example we are going to run one HTTP request for each row of a source
data frame. Let's suppose that all we have are the user IDs coming from a file
or another database:

```scala
val sourceDf = almaren.builder.sourceSql("select explode(array(1,2,3,4,5,6,7,8,9,10)) as __ID__").sqlExpr("__ID__", s"concat('$baseUrl',__ID__) as __URL__")
```

Note that `sourceDf` is not a `DataFrame` object, but instead
a `Option[com.github.music.of.the.ainur.almaren.Tree]` object. In order
to get access to the real `DataFrame` object and be able to  inspect its
content you need to call `batch` on it.

So let's take a look on what we got:

```
scala> sourceDf.batch.show()
+------+--------------------+
|__ID__|             __URL__|
+------+--------------------+
|     1|https://jsonplace...|
|     2|https://jsonplace...|
|  ... |       ...          |        
|    10|https://jsonplace...|
+------+--------------------+
```

To make an HTTP request we just need to call `http()` with the desired params:

```scala
val responseDf = sourceDf.http(method = "GET")
```

Let's see the content of `responseDf`:

```
scala> responseDf.batch.show()
21/08/24 08:34:49 WARN SparkSession$Builder: Using an existing SparkSession; some spark core configurations may not take effect.
+------+--------------+--------------------+---------------+--------------+---------+----------------+--------------------+
|__ID__|      __BODY__|          __HEADER__|__STATUS_CODE__|__STATUS_MSG__|__ERROR__|__ELAPSED_TIME__|             __URL__|
+------+--------------+--------------------+---------------+--------------+---------+----------------+--------------------+
|     1|{"id": 1,"n...|[etag -> [W/"1fd-...|            200|            OK|     null|             109|https://jsonplace...|
|     2|{"id": 2,"n...|[etag -> [W/"1fd-...|            200|            OK|     null|              63|https://jsonplace...|
|   ...|      ...     |         ...        |            ...|           ...|      ...|             ...|         ...        |
|    10|{"id": 10,"...|[etag -> [W/"1ff-...|            200|            OK|     null|              64|https://jsonplace...|
+------+--------------+--------------------+---------------+--------------+---------+----------------+--------------------+

```

We are interested on the JSON payload inside the `__BODY__` column so we need
to deserialized it first.

The framework is capable to infer the JSON schema to us but at a cost of waving
an additional batch of requests.

To prevent these additional calls each time our application runs, we need to
manually pass the JSON schema to our deserializer call.

We can write it manually or let the framework to the job for us with this
simple snippet:

```scala
val bodyDf = responseDf.batch.toDF
val jsonSchema = Util.genDDLFromJsonString(bodyDf, "__BODY__", 0.1)
```

Just type `jsonSchema` on spark-shell to see its content:

```
scala> val jsonSchema = Util.genDDLFromJsonString(bodyDf, "__BODY__", 0.1)
jsonSchema: String = `address` STRUCT<`city`: STRING, `geo`: STRUCT<`lat`: ...
```

Now let's use this `jsonSchema` and deserialize the payload:

```scala
val payload = responseDf.deserializer("JSON", "__BODY__", Some(jsonSchema))
```

And inspect the result:
```
scala> payload.batch.show()
+------+------------------------+--------------------+--------------------+--------------------+--------------------+
|__ID__|        many columns    |             __URL__|             address|                name|      more columns  |
+------+------------------------+--------------------+--------------------+--------------------+--------------------+
|     1|         some data      |https://jsonplace...|[Gwenborough, [-3...|       Leanne Graham|      more data     |
|     2|         some data      |https://jsonplace...|[Wisokyburgh, [-4...|        Ervin Howell|      more data     |
|   ...|            ...         |        ...         |         ...        |          ...       |         ...        |
|    10|         some data      |https://jsonplace...|[Lebsackbury, [-3...|  Clementina DuBuque|      more data     |
+------+------------------------+--------------------+--------------------+--------------------+--------------------+
```

Note that for "scalar" fields like `name` we got the plain values directly,
but the fields with nested values we need some additional work.

And is for these nested values that the DSL shines since we just need to
provide a simple mapping for each field:

`nested.json.path` + `dolar sign` + `my_new_column_name` + `colon sigh` + `TypeName`

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

val users = payload.dsl(kenyaDsl)
```

And let's also see the result:

```
scala> users.batch.show()
+---+--------------------+----------------+-----------------+-------------+--------------+
| id|           full_name|    many columns|   address_street|address_suite| more columns |
+---+--------------------+----------------+-----------------+-------------+--------------+
|  1|       Leanne Graham|       ...      |      Kulas Light|     Apt. 556|      ...     |
|  2|        Ervin Howell|       ...      |    Victor Plains|    Suite 879|      ...     |
|...|          ...       |       ...      |        ...      |      ...    |      ...     |
| 10|  Clementina DuBuque|       ...      |  Kattie Turnpike|    Suite 198|      ...     |
+---+--------------------+----------------+-----------------+-------------+--------------+
```

Now all user data is flattened and sending it to Postgres is pretty trivial.
We just need to use the JDBC Connector:

```scala
val dbHost = "localhost"
val dbPort = 5432
val dbUser = "mano"
val dbPass = "brau"
val dbName = "http_to_postgres_database"
val dbSchema = "http_to_postgres_schema"
val targetTable = "ingested_users"

val dbUrl = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
val dbDriver = "org.postgresql.Driver"
val fullTableName = s"$dbSchema.$targetTable"

val result = users.targetJdbc(dbUrl, dbDriver, fullTableName, SaveMode.Overwrite, Some(dbUser), Some(dbPass))
```

By calling `batch` on `result` we get the pipeline executed:
```
scala> result.batch
res3: org.apache.spark.sql.DataFrame = [id: bigint, full_name: string ... 13 more fields]
```

And finally let's get back to the `psql` prompt and check if the data was
properly inserted:

```
http_to_postgres_database=> select * from ingested_users;
 id |        full_name         | many columns | address_lat | address_lng |    company_name    
----+--------------------------+--------------+-------------+-------------+--------------------
  6 | Mrs. Dennis Schulist     | ...          | -71.4197    | 71.7478     | Considine-Lockman
  8 | Nicholas Runolfsdottir V | ...          | -14.3990    | -120.7677   | Abernathy Group
 ...|           ...            | ...          |     ...     |     ...     |      ...
  5 | Chelsey Dietrich         | ...          | -31.8129    | 62.5342     | Keebler LLC
(10 rows)
```

Note that we didn't create the table. Spark just did it for us. But the database
schema, into which the table was created, should be created before or the
ingestion will fail.

## Packaging this example on an application

All we need to build an application using `sbt` is a simple `build.sbt` file
listing all dependencies and a single `scala` file:

### build.sbt

The dependencies are pretty self-explanatory:

```sbt
name := "http-to-postgres-example"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "com.github.music-of-the-ainur" % "almaren-framework_2.11" % "0.9.0-2.4",
  "com.github.music-of-the-ainur" % "http-almaren_2.11" % "1.2.0-2.4",
  "org.postgresql" % "postgresql" % "42.2.23"
)
```

### HttpExample.scala

Put all code on a file at `./src/main/scala/com.modakanalytics/HttpExample.scala`:

```scala
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
```

### Generating the jar file

To generate the jar file, just type on terminal at the root directory of this
example:

```
user@host$ sbt package
[info] welcome to sbt 1.5.5 (AdoptOpenJDK Java 1.8.0_292)
[info] loading settings for project http-to-postgres-build from plugins.sbt ...
[info] loading project definition from /home/user/http-to-postgres/project
[info] loading settings for project http-to-postgres from build.sbt ...
[info] set current project to http-to-postgres-example (in build file:/home/user/http-to-postgres/)
[info] compiling 1 Scala source to /home/user/http-to-postgres/target/scala-2.11/classes ...
[success] Total time: 6 s, completed 24/08/2021 09:15:34
```

If all gone well, you will end up with a jar file at
`./target/scala-2.11/http-to-postgres-example_2.11-0.1.jar`.

You can run it from terminal with:

```
spark-submit \
  --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.9.0-2.4,com.github.music-of-the-ainur:http-almaren_2.11:1.2.0-2.4,org.postgresql:postgresql:42.2.23" \
  --class com.modakanalytics.HttpExample \
  --master local \
  ./target/scala-2.11/http-to-postgres-example_2.11-0.1.jar
```
