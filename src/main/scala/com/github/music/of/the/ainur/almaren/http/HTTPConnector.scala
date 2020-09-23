package com.github.music.of.the.ainur.almaren.http

import org.apache.spark.sql.{DataFrame,SaveMode}
import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.{Target,Source}

private[almaren] case class SourceHTTP(table:String, options:Map[String,String]) extends Source {
  def source(df: DataFrame): DataFrame = {
    logger.info(s"table:{$table}, options:{$options}")
    df.sparkSession.read.format("http")
      .options(options)
      .load(table)
  }
}

private[almaren] case class TargetHTTP(table:String, options:Map[String,String],saveMode:SaveMode) extends Target {
  def target(df: DataFrame): DataFrame = {
    logger.info(s"table:{$table}, options:{$options}")
    df.write.format("http")
      .options(options)
      .mode(saveMode)
      .save(table)
    df
  }
}

private[almaren] trait HTTPConnector extends Core {
  def targetHTTP(table:String, options:Map[String,String] = Map(),saveMode:SaveMode = SaveMode.ErrorIfExists): Option[Tree] =
     TargetHTTP(table,options,saveMode)

  def sourceHTTP(table:String, options:Map[String,String] = Map()): Option[Tree] =
    SourceHTTP(table,options)
}

object HTTP {
  implicit class HTTPImplicit(val container: Option[Tree]) extends HTTPConnector
}
