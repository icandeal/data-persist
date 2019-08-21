package com.stock

import java.io.FileInputStream
import java.util.Properties

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer
import scala.collection.convert.wrapAsJava._
/**
 * Hello world!
 *
 */
object App {

  case class History()

  def main(args: Array[String]): Unit = {

    val prop = new Properties()
//    prop.load(new FileInputStream(args(0)))

    prop.put("bootstrap.servers", "cdh:9092")
    prop.put("group.id", "test")
    prop.put("offset.reset", "latest")
    prop.put("topics", "history_data")
    prop.put("hbase.zookeeper.quorum", "cdh")
    prop.put("hbase.zookeeper.port", "2181")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> prop.getProperty("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> prop.getProperty("group.id"),
      "auto.offset.reset" -> prop.getProperty("offset.reset"),
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sparkConf = new SparkConf().setAppName("data-persist")
//      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topics = prop.getProperty("topics").split(",").toList
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.collect().foreach(println)
      rdd.foreachPartition(x =>  {
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, prop.getProperty("hbase.zookeeper.quorum"))
        hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, prop.getProperty("hbase.zookeeper.port"))

        val conn = ConnectionFactory.createConnection(hbaseConf)
        val table = conn.getTable(TableName.valueOf("history_data"))
        val putList = ListBuffer[Put]()
        x.foreach(data => {
          val json = new JSONObject(data.value())
          val conid = json.get("conid").toString
          val symbol = json.get("symbol").toString
          val exchange = json.get("exchange").toString
          val currency = json.get("currency").toString
          val secType = json.get("secType").toString
          val barSize = json.get("barSize").toString
          val duration = json.get("duration").toString
          val durationUnit = json.get("durationUnit").toString
          val whatToShow = json.get("whatToShow").toString
          val time = json.get("time").toString
          val count = json.get("count").toString
          val low = json.get("low").toString
          val high = json.get("high").toString
          val open = json.get("open").toString
          val close = json.get("close").toString
          val volume = json.get("volume").toString

          val put = new Put(Bytes.toBytes(s"${conid}_${symbol}_${whatToShow}_${barSize}_${time}"))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("conid"), Bytes.toBytes(conid))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("symbol"), Bytes.toBytes(symbol))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("exchange"), Bytes.toBytes(exchange))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("currency"), Bytes.toBytes(currency))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("secType"), Bytes.toBytes(secType))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("barSize"), Bytes.toBytes(barSize))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("duration"), Bytes.toBytes(duration))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("durationUnit"), Bytes.toBytes(durationUnit))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("whatToShow"), Bytes.toBytes(whatToShow))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("time"), Bytes.toBytes(time))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("count"), Bytes.toBytes(count))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("low"), Bytes.toBytes(low))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("high"), Bytes.toBytes(high))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("open"), Bytes.toBytes(open))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("close"), Bytes.toBytes(close))
          put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("volume"), Bytes.toBytes(volume))
          putList += put
        })

        table.put(putList)
        table.close()
      })

      /**  存hive  **/
//      val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

//      val st = StructType(Seq(
//        StructField("low", DoubleType, false),
//        StructField("high", DoubleType, false),
//        StructField("count", LongType, false),
//        StructField("time", StringType, false)
//      ))
//
//      val rowRdd = rdd.map(x => {
//        val json = new JSONObject(x)
//        Row(json.get("low").toString.toDouble, json.get("high").toString.toDouble,
//          json.get("count").toString.toLong, json.get("time").toString)
//      })
//      spark.createDataFrame(rowRdd, st).write.format("orc").mode(
//        SaveMode.Append
//      ).partitionBy("c_date").saveAsTable("tablename")
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
