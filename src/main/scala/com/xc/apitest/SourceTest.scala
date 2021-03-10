package com.xc.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.immutable
import scala.util.Random


/**
 * 1.从集合或文件读取数据
 * 2.从Kafka读取数据
 * 3.自定义Source，继承SourceFunction，读取数据
 */
object SourceTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataList: List[SensorReading] = List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    )

    //1.从集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(dataList)

    //stream1.print("stream1：").setParallelism(1)

    //2.从文件中读取数据
    val inputPath: String = "E:\\WorkSpace\\IdeaProjects\\FlinkLearn\\src\\main\\resources\\sensorreading.txt"
    val stream2: DataStream[String] = env.readTextFile(inputPath)
    //stream2.print("stream2：")//.setParallelism(1)

    //3.从Kafka中读取数据
    //需要引入flink-connector-kafka连接器的依赖

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers","")
    properties.setProperty("group.id","")

    val stream3: DataStream[String] = env.addSource( // 添加Source通用方法
      new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties)
    )
    //stream3.print("stream3：")

    //自定义Source
    val myStream: DataStream[SensorReading] = env.addSource(new MySensorSource)

    myStream.print("MySensorSource：").setParallelism(1)

    env.execute("SourceTest") //执行流式环境

  }
}

/**
 * 自定义Source，随机生成SensorReading传感器数据
 * 继承SourceFunction，实现cancel、run方法
 */
class MySensorSource extends SourceFunction[SensorReading]{

  // flag,表示数据源是否还在正常运行
  var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val random: Random = new Random()

    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 65 + random.nextGaussian() * 20)
    )

    while (running) {
      //更新温度值
      curTemp = curTemp.map(
        t => (t._1,t._2 + random.nextGaussian())
      )

      //获取当前时间戳
      val curTime: Long = System.currentTimeMillis()

      //发送数据
      curTemp.foreach {
        t => ctx.collect(SensorReading(t._1,curTime,t._2))
      }

      Thread.sleep(500) //休眠500ms

    }

  }

  override def cancel(): Unit = running = false
}

//定义样例类，传感器ID，时间戳，温度
case class SensorReading(id: String,timestamp: Long,temperature :Double)