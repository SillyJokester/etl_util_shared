package com.fxiaoke.dataplatform.util.app

import com.fxiaoke.dataplatform.util.common.{Logging, InitialLocalContext, InitialYarnContext}
import com.fxiaoke.dataplatform.util.tools.{SqlGenerator,DataFrameOperator}
import com.fxiaoke.dataplatform.scala.common.util.exception.ExceptionUtil.tryFunc
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import scala.collection.JavaConverters._


/**
  * Created by yaozy on 2016/8/25.
  */

/**
  *
  */
object Etl_Hj_Xt_Feed extends InitialYarnContext with Logging{
  val HIVE_XT_FEED_SOURCE_DB = "core_business"
  val HIVE_BASIC_SOURCE_DB = "ml_test"
  val HIVE_RAW_DATA_DB = "ml_raw"
  val HDFS_MAIN_DATA_PATH = "hdfs:///user/yaozy/core-business/xt/feed_accum_fundamentals"
  val HDFS_BASIC_DATA_PATH = "hdfs:///user/yaozy/core-business/xt/feed_avg_fundamentals"
  val HIVE_XT_BASIC_TABLE = "xt_feed_avg_fundamentals"

  /**
    * Load data from hive subject to given table name and given date. Specifically, count(*) operation will be executed
    * based group by IDs. e.g. select eid, count(*) from table1 where dt = "20160801" group by eid
    *
    * @param asId alias for column of count(*)， currently， only one as Id is allowed.
    * @param dt date constraint.
    * @param tableName table name of data source.
    * @param groupBYIds IDs as basis for group by operation.
    * @return
    */
  private def getDataFrameFromHiveWithGroupBy(asId:String,
                                              dt: String,
                                              tableName: String,
                                              groupBYIds: Seq[String] = Seq("eid")): Option[DataFrame]={
    val querySQL = SqlGenerator.getHiveSQLForGroupBy(dt,tableName,Seq(asId),Seq("*"), Seq(), Seq(), groupBYIds)
    info("SQL for loading " + tableName + " is: " + querySQL)
    tryFunc(hiveContext.sql(querySQL), "Failed in loading data from " + tableName)
  }

  /**
    * load data from Hive subject to given table name and where constraints
    *
    * @param colNames Columns names of loaded data in Hive
    * @param tableNames Names of Hive table
    * @param fromWhere Where constraints
    * @return
    */
  private def getDataFrameFromHive(colNames: Seq[String],
                                   tableNames: Seq[String],
                                   fromWhere: String = ""): Option[DataFrame] ={
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


    val xtFeedBackBasicDataFrame = getDataFrameFromHiveWithGroupBy("xt_feedback_num", dt,
      HIVE_XT_FEED_SOURCE_DB + "." + "feed_action_feed_reply")
    val xtLikeBasicDataFrame = getDataFrameFromHiveWithGroupBy("xt_like_num", dt,
      HIVE_XT_FEED_SOURCE_DB + "." + "feed_action_feed_like")
    val xtAtBasicDataFrame = getDataFrameFromHiveWithGroupBy("xt_at_num", dt,
      HIVE_XT_FEED_SOURCE_DB + "." + "feed_at")
    val xtRewardBasicDataFrame = getDataFrameFromHiveWithGroupBy("xt_reward_num", dt,
      HIVE_XT_FEED_SOURCE_DB + "." + "feed_action_reward")
    val xtRedPacketBasicDataFrame = getDataFrameFromHiveWithGroupBy("xt_redpacket_num", dt,
      HIVE_XT_FEED_SOURCE_DB + "." + "luck_money")

    val xtBasicDataFrameMap = Map(
      "xt_feedback_num" -> xtFeedBackBasicDataFrame,
      "xt_like_num" -> xtLikeBasicDataFrame,
      "xt_at_num" -> xtAtBasicDataFrame,
      "xt_reward_num" -> xtRewardBasicDataFrame,
      "xt_redpacket_num" -> xtRedPacketBasicDataFrame)

    // Execute full outer join on xtBasicDataFrames. null will be filled with 0
    val xtDailyBasicDataFrame: DataFrame = DataFrameOperator.multipleDataFrameOuterJoin(xtBasicDataFrameMap)
    xtDailyBasicDataFrame.show(10)

    // Calculate user-average features using enterprise.accountusedamount as number of active users.
    val entCols = Seq("eid","enterpriseaccount","accountusedamount")
    var enterpriseDataFrame = getDataFrameFromHive(entCols, Seq(HIVE_BASIC_SOURCE_DB + "." + "enterprise")).orNull
    if (enterpriseDataFrame != null) {
      enterpriseDataFrame = enterpriseDataFrame.withColumnRenamed("enterpriseaccount", "ea")
      val xtRawData = xtDailyBasicDataFrame.join(enterpriseDataFrame, Seq("eid"), "inner").na.drop("any")
      val xtDailyDataFrame = xtRawData.
        withColumn("xt_feedback_num_avg", xtRawData("xt_feedback_num") / xtRawData("accountusedamount")).
        withColumn("xt_like_num_avg", xtRawData("xt_like_num") / xtRawData("accountusedamount")).
        withColumn("xt_at_num_avg", xtRawData("xt_at_num") / xtRawData("accountusedamount")).
        withColumn("xt_reward_num_avg", xtRawData("xt_reward_num") / xtRawData("accountusedamount")).
        withColumn("xt_redpacket_num_avg", xtRawData("xt_reward_num") / xtRawData("accountusedamount")).
        withColumn("actiondate", xtRawData("xt_reward_num") * 0 + dt).
        na.fill(0)
      xtRawData.show(10)
      xtDailyDataFrame.show(10)
      //val filepath = Seq(HDFS_MAIN_DATA_PATH, dt.substring(0,3), dt.substring(4,5), dt.substring(6,7)).mkString("/")
      //val filepath = Seq("hdfs:///user/yaozy/testdata",dt.substring(0,4), dt.substring(4,6), dt.substring(6,8)).mkString("/")
      val filepath = "hdfs:///user/yaozy/core-business/xt/feed_avg_fundamentals/2016/08/10"
      import hiveContext.implicits._

      hiveContext.sql("USE " + HIVE_RAW_DATA_DB)
      /*
      hiveContext.sql("ALTER TABLE " + HIVE_XT_BASIC_TABLE + " ADD IF NOT EXISTS PARTITION(`day`=\'" + "20160810" + "\') " +
        "LOCATION " + "\'" + filepath + "\'")
      xtDailyDataFrame.registerTempTable("xt_basic_table")
      */
      /*
      hiveContext.sql("INSERT INTO xt_feed_avg_fundamentals PARTITION(`day`='2016-08-10') SELECT eid, ea, actiondate," +
        "xt_at_num, xt_at_num_avg, xt_feedback_num, xt_feedback_num_avg, xt_like_num, xt_like_num_avg," +
        "xt_redpacket_num, xt_redpacket_num_avg, xt_reward_num, xt_reward_num_avg from xt_basic_table")
      */

      xtDailyDataFrame.coalesce(1).write.mode(SaveMode.Append).saveAsTable("test")
      /*
      xtDailyDataFrame.select("eid","ea","actiondate","xt_at_num", "xt_at_num_avg", "xt_feedback_num",
        "xt_feedback_num_avg", "xt_like_num", "xt_like_num_avg", "xt_redpacket_num", "xt_redpacket_num_avg",
        "xt_reward_num", "xt_reward_num_avg").
        write.format("org.apache.hadoop.mapred.TextInputFormat").
        save(filepath)
      */
      //xtDailyBasicDataFrame.coalesce(1).write.text(filepath)
      //sqlContext.sql("use " + HIVE_BASIC_SOURCE_DB)

      // Calculate accumulated xt features


    } else {
      error("enterprise data is not loaded properly.")
    }
  }
}

