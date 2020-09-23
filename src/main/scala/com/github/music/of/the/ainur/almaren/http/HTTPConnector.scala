package com.github.music.of.the.ainur.almaren.http

import org.apache.spark.sql.{DataFrame,SaveMode}
import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.Main
import requests.Session

private[almaren] case class HTTP(headers:Map[String,String], url:String, method:String)(implicit session:Session) extends Main {
  override def core(df: DataFrame): DataFrame = {
//    logger.info(s"table:{$table}, options:{$options}")
  
    import df.sparkSession.implicits._
    df.mapPartitions(partition => {
      partition.map(row => {
        row.getAs[String]("foo")
         ""
      })
    })
    df
  }
}

private[almaren] trait HTTPConnector extends Core {
  def HTTP(headers:Map[String,String], url:String, method:String)(implicit session:Session): Option[Tree] =
     HTTP(headers,url,method)
}

object HTTP {
  implicit class HTTPImplicit(val container: Option[Tree]) extends HTTPConnector
}
