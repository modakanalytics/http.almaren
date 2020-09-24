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


private[almaren] case class MainHTTP(
  params:Map[String,String],
  url:String, 
  method:String,
  requestHandler:(Row,Session,String,Map[String,String],String) => requests.Response,
  session:() => requests.Session) extends Main {

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"params:{$params}, url:{$url}, method:{$method}")

    import df.sparkSession.implicits._
     
    val result = df.mapPartitions(partition => {
      val s = session()
      partition.map(row => {
        val response = Try(requestHandler(row,s,url,params,method))
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

  def http( 
    params:Map[String,String] = Map(),
    url:String,
    method:String,
    requestHandler:(Row,Session,String,Map[String,String],String) => requests.Response = HTTP.defaultHandler,
    session:() => requests.Session = () => requests.Session()): Option[Tree] =
    MainHTTP(params,url,method,requestHandler,session)
  
}

object HTTP {
  val defaultHandler = (row:Row,session:Session,url:String, params:Map[String,String], method:String) => {
    val paramsWithPlaceHoplder = replacePlaceHolderMap(row,params)
    val urlWithPlaceHolder = replacePlaceHolder(row,url)
    val data = row.getAs[String](Util.DataCol)
    method.toUpperCase match {
      case "GET" => session.get(urlWithPlaceHolder, params = paramsWithPlaceHoplder)
      case "POST" => session.post(urlWithPlaceHolder, params = paramsWithPlaceHoplder, data = data)
      case method => throw new Exception(s"Invalid Method: $method")
    }
  }

  val patternPlaceHolder = "%(\\w+)%".r

  private def replacePlaceHolderMap(row:Row,params:Map[String,String]): Map[String,String] =
    params.map( kv => {
      replacePlaceHolder(row,kv._1) -> replacePlaceHolder(row,kv._2)
    })

  private def replacePlaceHolder(row:Row,text:String): String = {
    text match {
      case patternPlaceHolder(rowKey) => text.replaceAllLiterally(s"%$rowKey%", row.getAs[Any](rowKey).toString())
      case _ => text
    }
    
  }

  implicit class HTTPImplicit(val container: Option[Tree]) extends HTTPConnector
}
