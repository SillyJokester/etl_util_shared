package com.fxiaoke.dataplatform.util.common

import com.github.autoconf.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConverters._

/**
  * Created by wujing on 2016/7/16.
  */

trait ConfigCenter {
  if (System.getProperty("spring.profiles.active") == null) {
    System.setProperty("spring.profiles.active", "firstshare")
  }
  //System.setProperty("spring.profiles.active", "firstshare")
  val esConfig = ConfigFactory.getConfig("elasticsearch-spark").getAll.asScala

  def initEsEnv(sc: SparkConf) = {
    esConfig.keys.filter(!_.isEmpty).filterNot(key => esConfig(key).isEmpty).foreach {
      key => sc.set(key, esConfig(key))
    }
    sc
  }
}

trait MLPConfigCenter {
  if (System.getProperty("spring.profiles.active") == null) {
    System.setProperty("spring.profiles.active", "firstshare")
  }
  val mlpConfig = ConfigFactory.getConfig("mlplatform").getAll.asScala
  mlpConfig.keys.filterNot(_.isEmpty).filterNot(key => mlpConfig(key).isEmpty)
  val esDir = mlpConfig.getOrElse("esDir", "yaozy/test")
  val esEstimationResultsDir = mlpConfig.getOrElse("esEstimationResultDir", "yaozy/estimationresults")
  val esTrainingHistoryDir = mlpConfig.getOrElse("esTrainingHistoryDir", "yaozy/traininghistory")
  val esTrainingInfoDir = mlpConfig.getOrElse("esTrainingInfoDir", "yaozy/traininginfo")
  val esTrainingResultsDir = mlpConfig.getOrElse("esTrainingResultsDir", "yaozy/trainingresults")
  val modelStoredDir = mlpConfig.getOrElse("modelStoreDir", "hdfs:///user/yaozy/result")
}

/**
  * 构造方法一
  */
trait InitialLocalContext {
  SparkContextFactory.initSparkContext("local", "test")
  val sc = SparkContextFactory.getSparkConf
  val sqlContext = SparkContextFactory.getSQLContext
  val hiveContext = SparkContextFactory.getHiveContext
}

trait InitialYarnContext {
  SparkContextFactory.initSparkContext("yarn-cluster", "MLTrainerTest")
  val sc = SparkContextFactory.getSparkConf
  val sqlContext = SparkContextFactory.getSQLContext
  val hiveContext = SparkContextFactory.getHiveContext
}

/**
  * 构造方法二
  */
abstract class AbstractYarnContext(master: String, app: String) extends ConfigCenter {
  SparkContextFactory.initSparkContext(master, app)
  val sc = SparkContextFactory.getSparkConf
  val sqlContext = SparkContextFactory.getSQLContext
}

object SparkContextFactory extends ConfigCenter {
  var sparkConf: SparkConf = null
  var sc: SparkContext = null
  var sqlContext: SQLContext = null
  var hiveContext: HiveContext = null

  def initSparkContext(master: String, app: String) = {
    if (sparkConf == null) {
      sparkConf = new SparkConf().setMaster(master).setAppName(app)
      sc = new SparkContext(initEsEnv(sparkConf))
      sqlContext = new SQLContext(sc)
      hiveContext = new HiveContext(sc)
    }
  }

  def getSparkConf = sc

  def getSQLContext = sqlContext

  def getHiveContext = hiveContext
}

/**
  * 构造方法三
  */
object StaticSparkContextFactory {

  val sparkConf = new SparkConf().setMaster("yarn-cluster").setAppName("test")
    .set("es.nodes", "172.31.101.13").set("es.port", "9200")
    .set("es.index.auto.create", "true").set("es.nodes.wan.only", "true")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def getSparkConf = sc

  def getSQLContext = sqlContext
}
