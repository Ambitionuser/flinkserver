package com.flink.it

import java.util.Properties

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaFlinkConsumer {


  def main(args: Array[String]): Unit = {
    println("start")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //kafka配置
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "imoc1:9092")
    prop.setProperty("zookeeper.connect", "imoc1:2181")
    prop.setProperty("group.id", "console-consumer-48145")

//    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer09[String]("abc", new SimpleStringSchema(), prop))

    val transaction: DataStream[String] = env.addSource(
        new FlinkKafkaConsumer09[String]("abc", new SimpleStringSchema(), prop)
      )
    transaction.print()
    println("stop")
    //启动程序
    env.execute("test kafka")

  }

}
