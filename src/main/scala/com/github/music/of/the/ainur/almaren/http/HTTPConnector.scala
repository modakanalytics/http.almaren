package com.github.music.of.the.ainur.almaren.http

import java.util.concurrent.Executors

import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.Main
import org.apache.spark.sql.{DataFrame, Row}
import requests.Session

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

private[almaren] final case class Result(
  `__ID__`:String,
  `__BODY__`:Option[String] = None,
  `__HEADER__`:Map[String,Seq[String]] = Map(),
  `__STATUS_CODE__`:Option[Int] = None,
  `__STATUS_MSG__`:Option[String] = None,
  `__ERROR__`:Option[String] = None,
  `__ELAPSED_TIME__`:Long
)

object Alias {
  val DataCol = "__DATA__"
  val IdCol = "__ID__"
  val UrlCol = "__URL__"
}


private[almaren] case class MainHTTP(
  headers:Map[String,String],
  method:String,
  requestHandler:(Row,Session,String,Map[String,String],String,Int,Int) => requests.Response,
  session:() => requests.Session,
  connectTimeout:Int,
  readTimeout:Int,
  threadPoolSize:Int,
  batchSize:Int) extends Main {

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"headers:{$headers}, method:{$method}, connectTimeout:{$connectTimeout}, readTimeout{$readTimeout}, threadPoolSize:{$threadPoolSize}")
    
    import df.sparkSession.implicits._

    val result = df.mapPartitions(partition => {

      implicit val ec:ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadPoolSize))
      val data:Iterator[Future[Seq[Result]]] = partition.grouped(batchSize).map(rows => Future {
        val s = session()
        rows.map(row => request(row,s))
      })
      val requests:Future[Iterator[Seq[Result]]] = Future.sequence(data)
      Await.result(requests,Duration.Inf).flatMap(s => s)
    })
    result.toDF
  }

  private def request(row:Row,session:Session): Result = {
    val url = row.getAs[Any](Alias.UrlCol).toString()
    val startTime = System.currentTimeMillis()
    val response = Try(requestHandler(row,session,url,headers,method,connectTimeout,readTimeout))
    val elapsedTime = System.currentTimeMillis() - startTime
    val id = row.getAs[Any](Alias.IdCol).toString()
    response match {
      case Success(r) => Result(
        id,
        Some(r.text()),
        r.headers,
        Some(r.statusCode),
        Some(r.statusMessage),
        `__ELAPSED_TIME__` = elapsedTime)
      case Failure(f) => {
            logger.error("Almaren HTTP Request Error",f)
              Result(id,`__ERROR__` = Some(f.getMessage()), `__ELAPSED_TIME__` = elapsedTime)
      }
    }
  }
}

private[almaren] trait HTTPConnector extends Core {

  def http( 
    headers:Map[String,String] = Map(),
    method:String,
    requestHandler:(Row,Session,String,Map[String,String],String,Int,Int) => requests.Response = HTTP.defaultHandler,
    session:() => requests.Session = HTTP.defaultSession,
    connectTimeout:Int = 60000,
    readTimeout:Int = 1000,
    threadPoolSize:Int = 1,
    batchSize:Int = 5000): Option[Tree] =
    MainHTTP(
      headers,
      method,
      requestHandler,
      session,
      connectTimeout,
      readTimeout,
      threadPoolSize,
      batchSize
    )
  
}

object HTTP {
  val defaultHandler = (row:Row,session:Session,url:String, headers:Map[String,String], method:String,connectTimeout:Int, readTimeout:Int) => {
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

  val defaultSession = () => requests.Session()

  implicit class HTTPImplicit(val container: Option[Tree]) extends HTTPConnector
}
