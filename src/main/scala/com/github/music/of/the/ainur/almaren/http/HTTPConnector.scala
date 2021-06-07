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

final case class Response(
  `__ID__`:String,
  `__BODY__`:Option[String] = None,
  `__HEADER__`:Map[String,Seq[String]] = Map(),
  `__STATUS_CODE__`:Option[Int] = None,
  `__STATUS_MSG__`:Option[String] = None,
  `__ERROR__`:Option[String] = None,
  `__ELAPSED_TIME__`:Long,
   `__URL__`:String)

object Alias {
  val DataCol = "__DATA__"
  val IdCol = "__ID__"
  val UrlCol = "__URL__"
}


private[almaren] case class HTTP(
  headers: Map[String, String],
  params: Map[String, String],
  method: String,
  requestHandler: (Row, Session, String, Map[String, String], Map[String, String], String, Int, Int) => requests.Response,
  session: () => requests.Session,
  connectTimeout: Int,
  readTimeout: Int,
  threadPoolSize: Int,
  batchSize: Int) extends Main {

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"headers:{$headers},params:{$params}, method:{$method}, connectTimeout:{$connectTimeout}, readTimeout{$readTimeout}, threadPoolSize:{$threadPoolSize}, batchSize:{$batchSize}")

    import df.sparkSession.implicits._

    val result = df.mapPartitions(partition => {

      implicit val ec:ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadPoolSize))
      val data:Iterator[Future[Seq[Response]]] = partition.grouped(batchSize).map(rows => Future {
        val s = session()
        rows.map(row => request(row,s))
      })
      val requests:Future[Iterator[Seq[Response]]] = Future.sequence(data)
      Await.result(requests,Duration.Inf).flatMap(s => s)
    })
    result.toDF
  }

  private def request(row:Row, session:Session): Response = {
    val url = row.getAs[Any](Alias.UrlCol).toString()
    val startTime = System.currentTimeMillis()
    val response = Try(requestHandler(row,session,url,headers,params,method,connectTimeout,readTimeout))
    val elapsedTime = System.currentTimeMillis() - startTime
    val id = row.getAs[Any](Alias.IdCol).toString()
    response match {
      case Success(r) => Response(
        id,
        Some(r.text()),
        r.headers,
        Some(r.statusCode),
        Some(r.statusMessage),
        `__ELAPSED_TIME__` = elapsedTime,
        `__URL__` = url)
      case Failure(f) => {
        logger.error("Almaren HTTP Request Error", f)
        Response(id, `__ERROR__` = Some(f.getMessage()), `__ELAPSED_TIME__` = elapsedTime, `__URL__` = url)
      }
    }
  }
}

private[almaren] case class HTTPBatch(
  url: String,
  headers: Map[String, String],
  params: Map[String, String],
  method: String,
  requestHandler: (String, Session, String, Map[String, String], Map[String, String], String, Int, Int) => requests.Response,
  session: () => requests.Session,
  connectTimeout: Int,
  readTimeout: Int,
  batchSize: Int,
  batchDelimiter: (Seq[Row]) => String
) extends Main {

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"url:{$url}, headers:{$headers},params:{$params}, method:{$method}, connectTimeout:{$connectTimeout}, readTimeout{$readTimeout}, batchSize:{$batchSize}")

    import df.sparkSession.implicits._

    val result = df.mapPartitions(partition => {
      partition.grouped(batchSize).flatMap(rows => {
        val s = session()
        val data = batchDelimiter(rows)
        val startTime = System.currentTimeMillis()
        Try{request(data,s)} match {
          case Success(r) => rows.map(row =>
            Response(
              row.getAs[Any](Alias.IdCol).toString(),
              Some(r.text()),
              r.headers,
              Some(r.statusCode),
              Some(r.statusMessage),
              `__ELAPSED_TIME__` = System.currentTimeMillis() - startTime,
              `__URL__` = url)
          )
          case Failure(f) => {
            logger.error("Almaren HTTP Request Error", f)
            rows.map(row => 
              Response(
                row.getAs[Any](Alias.IdCol).toString(),
                `__ERROR__` = Some(f.getMessage()),
                `__ELAPSED_TIME__` = System.currentTimeMillis() - startTime,
                `__URL__` = url)
            )
          }
        }
      })
    })
    result.toDF
  }

  private def request(data:String, session:Session): requests.Response = 
    requestHandler(data,session,url,headers,params,method,connectTimeout,readTimeout)


}

private[almaren] trait HTTPConnector extends Core {

  def http(
    headers: Map[String, String] = Map(),
    params: Map[String, String] = Map(),
    method: String,
    requestHandler: (Row, Session, String, Map[String, String], Map[String, String], String, Int, Int) => requests.Response = HTTP.defaultHandler,
    session: () => requests.Session = HTTP.defaultSession,
    connectTimeout: Int = 60000,
    readTimeout: Int = 1000,
    threadPoolSize: Int = 1,
    batchSize: Int = 5000): Option[Tree] =
    HTTP(
      headers,
      params,
      method,
      requestHandler,
      session,
      connectTimeout,
      readTimeout,
      threadPoolSize,
      batchSize
    )

  def httpBatch(
    url: String,
    headers: Map[String, String] = Map(),
    params: Map[String, String] = Map(),
    method: String,
    requestHandler: (String, Session, String, Map[String, String], Map[String, String], String, Int, Int) => requests.Response = HTTP.defaultHandlerBatch,
    session: () => requests.Session = HTTP.defaultSession,
    connectTimeout: Int = 60000,
    readTimeout: Int = 1000,
    batchSize: Int = 5000,
    batchDelimiter: (Seq[Row]) => String = HTTP.defaultBatchDelimiter
  ): Option[Tree] =
    HTTPBatch(
      url,
      headers,
      params,
      method,
      requestHandler,
      session,
      connectTimeout,
      readTimeout,
      batchSize,
      batchDelimiter
    )
}

object HTTP {
  val defaultHandler = (row:Row, session:Session, url:String, headers:Map[String, String], params:Map[String, String], method:String, connectTimeout:Int, readTimeout:Int) => {
    method.toUpperCase match {
      case "GET" => session.get(url, headers = headers, params = params, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "DELETE" => session.delete(url, headers = headers, params = params, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "OPTIONS" => session.options(url, headers = headers, params = params, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "HEAD" => session.head(url, headers = headers, params = params, readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "POST" => session.post(url, headers = headers, params = params, data = row.getAs[String](Alias.DataCol), readTimeout = readTimeout, connectTimeout = connectTimeout)
      case "PUT" => session.put(url, headers = headers, params = params, data = row.getAs[String](Alias.DataCol), readTimeout = readTimeout, connectTimeout = connectTimeout)
      case method => throw new Exception(s"Invalid Method: $method")
    }
  }

  val defaultHandlerBatch = (data:String, session:Session, url:String, headers:Map[String, String], params:Map[String, String], method:String, connectTimeout:Int, readTimeout:Int) => {
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

  val defaultBatchDelimiter = (rows:Seq[Row]) => rows.map(row => row.getAs[String](Alias.DataCol)).mkString("\n")

  val defaultSession = () => requests.Session()

  implicit class HTTPImplicit(val container: Option[Tree]) extends HTTPConnector
}
