package com.github.music.of.the.ainur.almaren.http

import org.apache.spark.sql.{DataFrame,SaveMode}
import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.Main
import requests.Session
import scala.util.{Success,Failure,Try}
import org.apache.spark.sql.Row

private[almaren] case class Result(
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
  params:Map[String,String],
  method:String,
  requestHandler:(Row,Session,String,Map[String,String],String) => requests.Response,
  session:() => requests.Session) extends Main {

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"params:{$params}, method:{$method}")

    import df.sparkSession.implicits._
     
    val result = df.mapPartitions(partition => {
      val s = session()
      partition.map(row => {
        val url = row.getAs[Any](Alias.UrlCol).toString()
        val startTime = System.currentTimeMillis()
        val response = Try(requestHandler(row,s,url,params,method))
        val elapsedTime = System.currentTimeMillis() - startTime
        val id = row.getAs[Any](Alias.IdCol).toString()
        response match {
          case Success(r) => Result(
            id,
            Some(r.text()),
            r.headers,
            Some(r.statusCode),
            Some(r.statusMessage),
            elapsedTime)
          case Failure(f) => {
            logger.error("Almaren HTTP Request Error",f)
            Result(id,`__ERROR__` = Some(f.getMessage()), `__ELAPSED_TIME__` = elapsedTime)
          }
        }
      })
    })
    result.toDF
  }
}

private[almaren] trait HTTPConnector extends Core {

  def http( 
    params:Map[String,String] = Map(),
    method:String,
    requestHandler:(Row,Session,String,Map[String,String],String) => requests.Response = HTTP.defaultHandler,
    session:() => requests.Session = () => requests.Session()): Option[Tree] =
    MainHTTP(params,method,requestHandler,session)
  
}

object HTTP {
  val defaultHandler = (row:Row,session:Session,url:String, params:Map[String,String], method:String) => {
    method.toUpperCase match {
      case "GET" => session.get(url, params = params)
      case "POST" => session.post(url, params = params, data = row.getAs[String](Alias.DataCol))
      case method => throw new Exception(s"Invalid Method: $method")
    }
  }

  implicit class HTTPImplicit(val container: Option[Tree]) extends HTTPConnector
}
