# HTTP to Postgres example

In this example we will show you how to use the HTTP Connector to ingest user
data from a REST API into a Postgres database.

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
container.

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

### Creating the schema

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

Open a new terminal and go to the root directory of this example.

You can open a spark shell by typing the command:

```
user@host$ spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.9.0-2.4,com.github.music-of-the-ainur:http-almaren_2.11:1.2.0-2.4,org.postgresql:postgresql:42.2.23"
```

After some initialization we get the scala prompt:

```
scala> 
```

This pipeline is pretty simple and could be written shortly by just chaining
each method with the following one but we are going to split them into
separated steps better explaining them.

We are also omitting the line breaks, so you can just copy-paste the commands
line by line.

To start, let's import the packages and get the `almaren` and `spark`
objects:

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import org.apache.spark.sql.SaveMode

val almaren = Almaren("HTTP to Postgres Example")

val spark = almaren.spark.master("local[*]").config("spark.sql.shuffle.partitions", "1").getOrCreate()
```

In our example we are going to run one HTTP request for each row of a source
data frame. Let's suppose that all we have are the user IDs coming from a file
or another database:

```scala
val sourceDf = almaren.builder.sourceDataFrame(spark.range(1, 11)).alias("SOURCE_DATA")
```

We can view the content of `sourceDF` by calling the methods `.batch.show()` on it:

```
scala> sourceDf.batch.show()
+---+
| id|
+---+
|  1|
|  2|
|...|
| 10|
+---+
```

But in order to make the HTTP request we must provide at leas the columns
`__ID__` and `__URL__`:

```scala
val baseUrl = "https://jsonplaceholder.typicode.com/users/"
val requestDf = sourceDf.sql(s"select id as __ID__, concat('$baseUrl', id) as __URL__ from SOURCE_DATA")
```

Let's see what we got:

```
scala> requestDf.batch.show()
+------+--------------------+
|__ID__|             __URL__|
+------+--------------------+
|     1|https://jsonplace...|
|     2|https://jsonplace...|
|  ... |       ...          |        
|    10|https://jsonplace...|
+------+--------------------+
```

Now let's do the requests and get the deserialized JSON data:

```scala
val payload = requestDf.http(method = "GET").deserializer("JSON", "__BODY__")
```

Let's take a look on what we got:
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
val users = payload.dsl(
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
```

And let's see the result:

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

Now sending this data to Postgres is pretty trivial using the JDBC Connector:

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

Let's call `batch` on `result` to exec the pipeline:
```
scala> result.batch
res3: org.apache.spark.sql.DataFrame = [id: bigint, full_name: string ... 13 more fields]
```

And finally let's get back to the `psql` prompt and check if the data was
properly inserted:

```
http_to_postgres_database=> select id, full_name, '...' as "many columns", address_lat, address_lng, company_name from ingested_users;
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

## The full pipeline condensed

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import org.apache.spark.sql.SaveMode

val almaren = Almaren("HTTP to Postgres Example")
val spark = almaren.spark
  .master("local[*]")
  .config("spark.sql.shuffle.partitions", "1")
  .getOrCreate()

val sourceDf = almaren.builder
  .sourceDataFrame(spark.range(1, 11))
  .alias("SOURCE_DATA")

sourceDf
  .sql(s"select id as __ID__, concat('https://jsonplaceholder.typicode.com/users/', id) as __URL__ from SOURCE_DATA")
  .http(method = "GET")
  .deserializer("JSON", "__BODY__")
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
  .targetJdbc(
    url = s"jdbc:postgresql://localhost/http_to_postgres_database",
    driver = "org.postgresql.Driver", "http_to_postgres_schema.ingested_users",
    saveMode = SaveMode.Overwrite,
    user = Some("mano"),
    password = Some("brau")
  )
  .batch
  .show()

```