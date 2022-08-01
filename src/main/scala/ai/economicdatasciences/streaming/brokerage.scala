package ai.economicdatasciences.sia.streaming

import ai.economicdatasciences.sia.spark.SparkStreamingInit.ssc
import ai.economicdatasciences.sia.model.Order

// import scala.util.{Try, Success, Failure}
import org.apache.spark.streaming.dstream.DStream

import java.time.Instant
import com.typesafe.config.ConfigFactory

class Brokerage {
  // get hdfs root
  val spark_hdfs =
    ConfigFactory.load("spark.properties").getString("spark.hdfs").replace(""""""","")
  // get file stream
  val filestream = ssc.textFileStream(s"${spark_hdfs}/orders")

  val orders: DStream[Order] = filestream.flatMap(line => {
    val s = line.split(",")
    try {
      assert(s(6) == "B" || s(6) == "S")
      List(Order(
        Instant.parse((s(0) + ".000Z").replace(" ","T")),
        s(1).toLong,
        s(2).toLong,
        s(3),
        s(4).toInt,
        s(5).toDouble,
        s(6) == "B"
      ))
    } catch {
      case e: Throwable => println("Wrong line format (" + e + "): " + line)
      List()
    }

  })

  // val orderStream = orders.filter(_ != None).flatMap(_.get)

  //
  // val numPerTypeMap: DStream[(Boolean, Long)] = orders.map(o => {
  //   (o.buy, 1L)
  //   // o.buy match {
  //   //   case true => ("TRUE", "1")
  //   //   case false => ("FALSE", "1")
  //   // }
  // })

  // .reduceByKey((c1: Long, c2: Long) => c1 + c2)

  // val numPerType: DStream[(Boolean, Long)] = numPerTypeMap.reduceByKey((c1: Long, c2: Long) => c1 + c2)

  // .groupBy(_._1).map(x => (x._1, x._2.length))

  // .reduceByKey((c1, c2) => c1 + c2)

  orders.repartition(1).saveAsTextFiles(s"${spark_hdfs}/orders/test", "txt")
}
