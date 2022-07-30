package ai.economicdatasciences.sia.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import com.typesafe.config.ConfigFactory

trait EDSspark {
  val spark_master =
    ConfigFactory.load("spark.properties").getString("spark.master")
  val spark_port =
    ConfigFactory.load("spark.properties").getString("spark.port")
  val spark_name =
    ConfigFactory.load("spark.properties").getString("spark.name")

  val sparkConf = new SparkConf().setMaster(spark_master + ":" + spark_port).setAppName(spark_name).set("spark.driver.memory", "2g").set("spark.executor.memory", "2g")

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.WARN)

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("WARN")
  spark.sparkContext.setLogLevel("WARN")

}
