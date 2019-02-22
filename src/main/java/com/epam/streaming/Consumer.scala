package com.epam.streaming

import java.util.concurrent._
import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._


class Consumer(val brokers: String,
               val groupId: String,
               val topic: String) {

  val props: Properties = createConsumerConfig(brokers, groupId)
  val kafkaConsumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null


  def shutdown(): Unit = {
    if (kafkaConsumer != null)
      kafkaConsumer.close()
    if (executor != null)
      executor.shutdown()
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-1")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    return props
  }

  def run(): Unit = {
    kafkaConsumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records: ConsumerRecords[String, String] = kafkaConsumer.poll(500)

          for (record <- records) {
            System.out.println("Received message: (" + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }
}
//Main
object Consumer extends App {

  //params for consumer
  val topic = "Elastic"
  val broker ="localhost:9092"
  val groupid ="consumer-1"

  //consumer
  val consumer = new Consumer(broker,groupid,topic)
  consumer.run()

  //running sparkJob for reading topic and write strokes to HDFS
  SparkJob.sparkJob()
}