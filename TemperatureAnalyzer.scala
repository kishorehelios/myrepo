package org.training.spark.loganalysis.analyzer

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by hduser on 6/16/19.
  */
object TemperatureAnalyzer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("spark://ubuntu:7077").setAppName("Temparature analyzer streaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, new Duration(10 * 1000))
    val zkQuorum="localhost:2181"
    val clientGroup="Temperature sensor"
    val topicMap="temp_log".split(",").map((_, 1)).toMap

    val logLinesDStream = KafkaUtils.createStream(
      ssc,            // StreamingContext object
      zkQuorum,
      clientGroup,
      topicMap
    ).map(x=>x)

    logLinesDStream.print()


    logLinesDStream.foreachRDD(accessLogs => {
      accessLogs.foreach(result => DataBase_Operations.Insert(result._1))
    })



    ssc.start()
    ssc.awaitTermination()


  }
}

object  DataBase_Operations {
  val sparkConf = new SparkConf().setMaster("spark://ubuntu:7077").setAppName("Temparature analyzer streaming")
  val sc = new SparkContext(sparkConf)

  def Insert(args:Float):Unit={
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dbtable=""
    if(args>=0){
       val dbtable="warm"
    } else{
       val dbtable="cold"
    }
    val mysqlOption = Map("url" -> "jdbc:mysql://hduser:3306/mysql",
      "dbtable" -> dbtable,
      "user" -> "hduser",
      "password" -> "training"
      )


    val jdbcDF = sqlContext.read
      .format("org.apache.spark.sql.jdbc")
      .options(mysqlOption)
      .load()

    sqlContext.sql("insert into "+dbtable+" (Date, Time, Year, TimeStamp, Temparature) VALUES (NOW(), NOW(), NOW(), NOW(), "+args+");").write.save("/home/hduser/insert.txt")

    val aggregatesum=sqlContext.sql("select sum(tmp) as agg from (SELECT  Temparature as tmp FROM mysql.warm WHERE TimeStamp >= DATE_SUB(NOW(), interval 1 hour)\nunion\nSELECT  Temparature as tmp FROM mysql.cold WHERE TimeStamp >= DATE_SUB(NOW(), interval 1 hour));").load()

    sqlContext.sql("insert into aggregate (Date, Time, Year, TimeStamp, Temparature) VALUES (NOW(), NOW(), NOW(), NOW(), "+aggregatesum.toFloat()+");").write.save("/home/hduser/insert.txt")

  }


}
