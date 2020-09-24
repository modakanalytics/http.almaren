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
  `__ERROR__`:Option[String] = None
)

object Util {
  val DataCol = "__DATA__"
  val IdCol = "__ID__"
}


private[almaren] case class HTTP(
  params:Map[String,String],
  url:String, 
  method:String,
  request_closure:(Row,Session,String,Map[String,String],String) => requests.Response,
  session:() => requests.Session) extends Main {

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"params:{$params}, url:{$url}, method:{$method}")

    import df.sparkSession.implicits._
     
    val result = df.mapPartitions(partition => {
      val s = session()
      partition.map(row => {
        val response = Try(request_closure(row,s,url,params,method))
        val id = row.getAs[Any](Util.IdCol).toString()
        response match {
          case Success(r) => Result(
            id,
            Some(r.text()),
            r.headers,
            Some(r.statusCode),
            Some(r.statusMessage))
          case Failure(f) => {
            logger.error("Almaren HTTP Request Error",f)
            Result(id,`__ERROR__` = Some(f.getMessage()))
          }
        }
      })
    })
    result.toDF
  }
}

private[almaren] trait HTTPConnector extends Core {

  val defaultHandler = (row:Row,session:Session,url:String, params:Map[String,String], method:String) => {
    val data = row.getAs[String](Util.DataCol)
    method.toUpperCase match {
      case "GET" => session.get(url, params = params)
      case "POST" => session.post(url, params = params, data = data)
      case method => throw new Exception(s"Invalid Method: $method")
    }
  }

  def http( 
    params:Map[String,String] = Map(),
    url:String,
    method:String,
    requestClosure:(Row,Session,String,Map[String,String],String) => requests.Response = defaultHandler,
    session:() => requests.Session = () => requests.Session()): Option[Tree] =
    HTTP(params,url,method,requestClosure,session)
  
}

object HTTP {
  implicit class HTTPImplicit(val container: Option[Tree]) extends HTTPConnector
}
