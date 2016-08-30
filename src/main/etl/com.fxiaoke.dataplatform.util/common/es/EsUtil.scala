package com.fxiaoke.dataplatform.util.common.es

import com.fxiaoke.dataplatform.util.common.InitialYarnContext
import org.elasticsearch.spark._

/**
  * Created by Lipy on 2016/7/5.
  * Utilities related to ElasticSearch
  */
object EsUtil extends InitialYarnContext {

  implicit def sparkMapFunctions(m: Map[String, Any]): SparkMapFunctions = new SparkMapFunctions(m)

  class SparkMapFunctions(dataMap: Map[String, Any]) extends Serializable {
    def saveToEs(resource: String) {
      sc.makeRDD(Seq(dataMap)).saveToEs(resource)
    }

    def saveToEs(resource: String, cfg: scala.collection.Map[String, String]) {
      sc.makeRDD(Seq(dataMap)).saveToEs(resource, cfg)
    }

    def saveToEs(cfg: scala.collection.Map[String, String]) {
      sc.makeRDD(Seq(dataMap)).saveToEs(cfg)
    }
  }

  def writeMapToES(dataMap: Map[String, Any], idxStr: String, typeStr: String, idStr: String): Unit = {
    sc.makeRDD(Seq(dataMap)).saveToEs(idxStr + "/" + typeStr, Map("es.mapping.id" -> idStr))
  }

  def writeMapToEs(dataMap: Map[String, Any], resource: String) = sc.makeRDD(Seq(dataMap)).saveToEs(resource)

  def writeMetrics(dataMap: Map[String, Any], idStr: String) = writeMapToES(dataMap, "mlp", "metrics", idStr)

  def main(args: Array[String]) {
    val numbers = Map("iteration" -> "70", "length" -> "2.5", "size" -> "3.5")
    writeMetrics(numbers, "iteration")
  }
}
