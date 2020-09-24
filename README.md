# HTTP Connector

[![Build Status](https://travis-ci.com/modakanalytics/http.almaren.svg?branch=master)](https://travis-ci.com/modakanalytics/http.almaren)

```
libraryDependencies += "com.github.music-of-the-ainur" %% "http-almaren" % "0.0.1-2.4"
```

```
spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.4.0-2.4,com.github.music-of-the-ainur:http-almaren_2.11:0.0.2-2.4"
```

## Source and Target

### Source 
#### Parameteres

| Parameters | Description             |
|------------|-------------------------|


#### Example


```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.http.HTTP.HTTPImplicit
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit

val almaren = Almaren("App Name")

val df =  almaren
         .builder

df.show(false)
```



### Target:
#### Parameters

| Parameters | Description             |
|------------|-------------------------|

#### Example

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.http.HTTP.HTTPImplicit
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import org.apache.spark.sql.SaveMode

val almaren = Almaren("App Name")
```

