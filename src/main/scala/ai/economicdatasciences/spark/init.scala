package ai.economicdatasciences.sia.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.log4j.{Level, Logger}

import com.typesafe.config.ConfigFactory

object SparkInit {
  val spark_master =
    ConfigFactory.load("spark.properties").getString("spark.master").replace(""""""","")
  val spark_port =
    ConfigFactory.load("spark.properties").getString("spark.port").replace(""""""","")
  val spark_name =
    ConfigFactory.load("spark.properties").getString("spark.name").replace(""""""","")

  val sparkInput = spark_port match {
    case "" => spark_master
    case _ => spark_master + ":" + spark_port
  }

  val sparkConf = new SparkConf().setMaster(sparkInput).setAppName(spark_name).set("spark.driver.memory", "2g").set("spark.executor.memory", "2g")

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.WARN)

  val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  sc.setLogLevel("WARN")
  spark.sparkContext.setLogLevel("WARN")

}
