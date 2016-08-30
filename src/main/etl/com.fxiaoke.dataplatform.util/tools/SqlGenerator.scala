package com.fxiaoke.dataplatform.util.tools

import com.fxiaoke.dataplatform.util.common.Logging
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

/**
  * Created by yaozy on 2016/8/26.
  */

object SqlGenerator extends Logging{
  def getHiveSQLForGroupBy(dt: String,
                           tableName: String,
                           countAsIds: Seq[String] = Seq(),
                           countCols: Seq[String] = Seq("*"),
                           sumAsIds: Seq[String] = Seq(),
                           sumCols: Seq[String] = Seq(),
                           groupBYIds: Seq[String] = Seq("eid")): String={
    assert(countAsIds.isEmpty || countCols.isEmpty || countAsIds.size == countCols.size,
      "countAasIds should be the same size as countCols if provided.")
    val countWords = if (countAsIds.isEmpty) {
      if (countCols.isEmpty) "" else "," + countCols.map("count(" + _ + ")").mkString(",")
    } else {
      if (countCols.isEmpty) "" else "," + countCols.zip(countAsIds).map(r=> "count(" + r._1 + ") AS " + r._2 ).mkString(",")
    }

    assert(sumAsIds.isEmpty || sumCols.isEmpty || sumAsIds.size == sumCols.size,
      "sumAsIds should be the same size as sumCols if provided.")
    val sumWords = if (sumAsIds.isEmpty) {
      if (sumCols.isEmpty) "" else "," + sumCols.map("sum(" + _ + ")").mkString(",")
    } else {
      if (sumCols.isEmpty) "" else "," + sumCols.zip(sumAsIds).map(r=> "sum(" + r._1 + ") AS " + r._2 ).mkString(",")
    }

    val dtWords = if (dt.isEmpty) "" else " `day` = " + dt
    val querySQL =  "SELECT " + groupBYIds.mkString(",") + countWords + sumWords +
      " FROM " + tableName +
      " WHERE " + dtWords + " GROUP BY " + groupBYIds.mkString(",")
    querySQL
  }

  def getSelectHiveSQL(colNames: Seq[String],
                       tableNames: Seq[String],
                       fromWhere: String = ""): String = {
    assert(!tableNames.isEmpty, "Table name should be provided.")
    val cols = if (colNames.isEmpty) "*" else colNames.mkString(",")
    val whereCons = if (fromWhere.isEmpty) "" else " WHERE " + fromWhere
    "SELECT " + cols + " FROM " + tableNames.mkString(",") + whereCons
  }

  def main(args: Array[String]): Unit = {
    val dt = if (args.length == 0) {
      LocalDate.now().minusDays(1).toString(DateTimeFormat.forPattern("yyyyMMdd"))
    } else {
      args(0)
    }
    val hivesql = getHiveSQLForGroupBy(dt,"tablename", Seq("`sum`"),Seq("*"),Seq("`count`"),Seq("*"))
    println(hivesql)
  }

}
