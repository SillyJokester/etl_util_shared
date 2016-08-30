package com.fxiaoke.dataplatform.util.app
/**
  * Created by yaozy on 2016/8/26.
  */

import com.fxiaoke.dataplatform.util.common.{Logging, InitialLocalContext, InitialYarnContext}
import com.fxiaoke.dataplatform.scala.common.util.exception.ExceptionUtil.tryFunc
import com.fxiaoke.dataplatform.util.tools.{SqlGenerator,DataFrameOperator}
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object Etl_Hj_Qx_Msg extends InitialYarnContext with Logging{
  val HIVE_QX_MSG_SOURCE_DB = "core_business"
  val HIVE_BASIC_SOURCE_DB = "ml_test"

  private def getDataFrameFromHiveWithGroupBy(countAsId:String,
                                              countTerm:String,
                                              dt: String,
                                              tableName: String,
                                              groupBYIds: Seq[String] = Seq("eid")): Option[DataFrame]={
    val querySQL = SqlGenerator.getHiveSQLForGroupBy(dt,tableName,Seq(countAsId),Seq(countTerm),Seq(),Seq(),groupBYIds)
    info("SQL for loading " + tableName + " is: " + querySQL)
    tryFunc(hiveContext.sql(querySQL), "Failed in loading data from " + tableName)
  }

  private def getDataFrameFromHive(colNames: Seq[String],
                                   tableNames: Seq[String],
                                   fromWhere: String = ""): Option[DataFrame] = {
    val querySQL = SqlGenerator.getSelectHiveSQL(colNames, tableNames, fromWhere)
    info("SQL for loading " + tableNames.mkString(",") + " is: " + querySQL)
    tryFunc(hiveContext.sql(querySQL), "Failed in loading data from " + tableNames.mkString(","))
  }

  def main(args: Array[String]): Unit = {
    // get ETL execution date. RULES as follows:
    // dt (the reference date for ETL job) equals the day before current date if no system arguments provided;
    // Otherwise, use the first one of the system arguments
    val dt = if (args.length == 0) {
      LocalDate.now().minusDays(1).toString(DateTimeFormat.forPattern("yyyyMMdd"))
    } else {
      args(0)
    }
    println(dt)

    val qxNewSessionBasicDataFrame = getDataFrameFromHiveWithGroupBy("qx_newsession_num", "distinct sessionID", dt,
      HIVE_QX_MSG_SOURCE_DB + "." + "new_session")
    val qxActiveSessionBasicDataFrame = getDataFrameFromHiveWithGroupBy("qx_activesession_num", "distinct sessionID", dt,
      HIVE_QX_MSG_SOURCE_DB + "." + "active_session")
    val qxSeedBasicDataFrame = getDataFrameFromHive(Seq("eid", "count", "messagetype"), Seq("message"), "`day`=" + dt).get
    qxSeedBasicDataFrame.registerTempTable("qx_seed_basic")
    val qxSeedTDataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedt_num FROM qx_seed_basic " +
      "WHERE messagetype = \"T\" Group BY eid")
    val qxSeedADataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seeda_num FROM qx_seed_basic " +
      "WHERE messagetype = \"A\" Group BY eid")
    val qxSeedIDataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedi_num FROM qx_seed_basic " +
      "WHERE messagetype = \"I\" Group BY eid")
    val qxSeedLDataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedl_num FROM qx_seed_basic " +
      "WHERE messagetype = \"L\" Group BY eid")
    val qxSeedDDataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedd_num FROM qx_seed_basic " +
      "WHERE messagetype = \"D\" Group BY eid")
    val qxSeedAVEventDataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedavevent_num FROM qx_seed_basic " +
      "WHERE messagetype = \"AVEvent\" Group BY eid")
    val qxSeedMeetingCardDataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedmeetingcard_num " +
      "FROM qx_seed_basic WHERE messagetype = \"MeetingCard\" Group BY eid")
    val qxSeedIGT4DataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedigt4_num FROM qx_seed_basic " +
      "WHERE messagetype = \"IGT@4\" Group BY eid")
    val qxSeedIGT5DataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedigt5_num FROM qx_seed_basic " +
      "WHERE messagetype = \"IGT@5\" Group BY eid")
    val qxSeedLWNDataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedlwn_num FROM qx_seed_basic " +
      "WHERE messagetype = \"LWN\" Group BY eid")
    val qxSeedLWV1DataFrame = sqlContext.sql("SELECT eid, sum(count) as qx_seedlwv1_num FROM qx_seed_basic " +
      "WHERE messagetype = \"LWV@1\" Group BY eid")

    val qxBasicDataFrameMap = Map(
      "qx_newsession_num" -> qxNewSessionBasicDataFrame,
      "qx_activesession_num" -> qxActiveSessionBasicDataFrame,
      "qx_seedt_num" -> qxSeedTDataFrame,
      "qx_seeda_num" -> qxSeedADataFrame,
      "qx_seedi_num" -> qxSeedIDataFrame,
      "qx_seedl_num" -> qxSeedLDataFrame,
      "qx_seedavevent_num" -> qxSeedAVEventDataFrame,
      "qx_seedmeetingcard_num" -> qxSeedMeetingCardDataFrame,
      "qx_seedigt4_num" -> qxSeedIGT4DataFrame,
      "qx_seedigt5_num" -> qxSeedIGT5DataFrame,
      "qx_seedlwn_num" -> qxSeedLWNDataFrame,
      "qx_seedlwv1_num" -> qxSeedLWV1DataFrame
    )

    //val qxBasicDataFrame = DataFrameOperator.multipleDataFrameOuterJoin(qxBasicDataFrameMap)



  }
}

