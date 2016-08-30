package com.fxiaoke.dataplatform.util.tools

import com.fxiaoke.dataplatform.scala.common.util.log.Logging
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer

/**
  * Created by yaozy on 2016/8/30.
  */

object DataFrameOperator extends Logging {
  /**
    * Combine multiple DataFrame using full outer join based on eid. null value will be filled by 0.
    *
    * @param xtBasicDataFrameMap Map of type (ID-> DataFrame) where ID in this case uses Hive table name
    * @return
    */
  def multipleDataFrameOuterJoin(xtBasicDataFrameMap: Map[String, Option[DataFrame]]): DataFrame = {
    val noneEmptyDataFrameArray = new ArrayBuffer[String]()
    val EmptyDataFrameArray = new ArrayBuffer[String]()
    for ((table, data) <- xtBasicDataFrameMap) {
      if (data.isEmpty) {
        EmptyDataFrameArray += table
        warn("No data loaded for " + table + ". " +
          "Either data is not ready or error may take place in loading procedure")
      } else {
        noneEmptyDataFrameArray += table
      }
    }
    if (noneEmptyDataFrameArray.nonEmpty) {
      var tmpDataFrame = xtBasicDataFrameMap.get(noneEmptyDataFrameArray(0)).get.get
      for (i <- 1 until noneEmptyDataFrameArray.size) {
        tmpDataFrame = tmpDataFrame.
          join(xtBasicDataFrameMap.get(noneEmptyDataFrameArray(i)).get.get, Seq("eid"), "outer")
      }
      if (EmptyDataFrameArray.nonEmpty) {
        for (table <- EmptyDataFrameArray) {
          tmpDataFrame = tmpDataFrame.withColumn(table, tmpDataFrame(noneEmptyDataFrameArray(0)) * 0)
        }
      }
      tmpDataFrame.na.fill(0)
    } else {
      error("All data requred to load is not available.")
      null
    }
  }
}
