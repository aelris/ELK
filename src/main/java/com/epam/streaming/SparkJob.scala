package com.epam.streaming

import java.time.{Instant, LocalDateTime}
import java.util.TimeZone

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.elasticsearch.spark.sql.EsSparkSQL


object SparkJob {


  def sparkJob() {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "localhost")
      .config("es.port", "9200")
      .config("es.http.timeout", "5m")
      .config("es.scroll.size", "50")
      .config("spark.es.nodes.client.only", "false")
      .config("es.spark.sql.streaming.sink.log.enabled", "false")
      .config("spark.sql.streaming.checkpointLocation", "/sinks/elasticsearch")
      .config("es.nodes.wan.only", "true")
      .getOrCreate()

    //DataFrame that reads kafka topic line by line
    var dataFrameKafkaRecords: DataFrame = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", Consumer.topic)
      .load()


    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()),
      TimeZone.getDefault().toZoneId())


    val structType1 = new StructType()
      .add("value", DataTypes.StringType)

    val structType2 = new StructType()
      .add("date", DataTypes.StringType)
      .add("value", DataTypes.StringType)


   var lines: Dataset[Row] = dataFrameKafkaRecords.map(row =>Row(row))(Encoders[Row])
    var timedLines: Dataset[(Row, String)] =lines.map(l => (l, time.toString))(Encoders.tuple(RowEncoder.apply(structType1), Encoders.STRING))


    EsSparkSQL.saveToEs(timedLines,"timed_elas/json")
  }

}
