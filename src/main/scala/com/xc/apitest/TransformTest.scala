package com.xc.apitest

import org.apache.flink.streaming.api.scala._


/**
 * 求传感器温度最小的记录数据
 */
object TransformTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //1.读取文件数据

    val inputPath: String = "E:\\WorkSpace\\IdeaProjects\\FlinkLearn\\src\\main\\resources\\sensorreading.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    //2.转换成样例类

    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    //3.分组聚合，输出每个传感器当前的最小温度值

    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy("id") //根据ID进行分组，键控流：DataStream -> KeyedStream
      .minBy("temperature") //求最小温度值，min与minBy区别：时间戳字段的变化

    //4.打印最小值

    aggStream.print()

    env.execute("TransformTest")
  }
}


