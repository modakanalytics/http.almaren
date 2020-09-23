package com.github.music.of.the.ainur.almaren.http

import org.apache.spark.sql.{DataFrame,SaveMode}
import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.Main
import requests.Session
import scala.util.{Success,Failure,Try}

private[almaren] case class Response(
  `__ID__`:String,
  `__BODY__`:Option[String] = None,
  `__HEADER__`:Map[String,Seq[String]] = Map(),
  `__STATUS_CODE__`:Option[Int] = None,
  `__STATUS_MSG__`:Option[String] = None,
  `__ERROR__`:Option[String] = None
)

private[almaren] case class HTTP(params:Map[String,String], url:String, method:String)(implicit session:Session) extends Main {

  val DataCol = "__DATA__"
  val IdCol = "__ID__"

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"params:{$params}, url:{$url}, method:{$method}")
  
    import df.sparkSession.implicits._
    df.mapPartitions(partition => {
      partition.map(row => {
        val data = row.getAs[String](DataCol)
        val response = Try(method.toUpperCase match {
          case "GET" => session.get(url, params = params, data = data)
          case "POST" => session.post(url, params = params, data = data)
          case method => throw new Exception(s"Invalid Method: $method")
        })
        val id = row.getAs[String](IdCol)
        response match {
          case Success(r) => Response(
            id,
            Some(r.text()),
            r.headers,
            Some(r.statusCode),
            Some(r.statusMessage))
          case Failure(f) => {
            logger.debug("Almaren HTTP Request Error",f)
            Response(id,`__ERROR__` = Some(f.getMessage()))
          }
        }
        
      })
    })
    df
  }
}

private[almaren] trait HTTPConnector extends Core {
  def HTTP(params:Map[String,String], url:String, method:String)(implicit session:Session): Option[Tree] =
     HTTP(params,url,method)
}

object HTTP {
  implicit class HTTPImplicit(val container: Option[Tree]) extends HTTPConnector
}
