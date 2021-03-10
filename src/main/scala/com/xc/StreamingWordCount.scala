package com.xc

import org.apache.flink.streaming.api.scala._ //隐式转换
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(2)

    //从端口获取数据
    val text: DataStream[String] = env.socketTextStream("172.26.0.250", 9998, '\n')

    //解析数据
    val windowCounts: DataStream[WordWithCount] = text
      .flatMap { words => words.split("\\s") }
      .map { word => WordWithCount(word, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")

  }

  case class WordWithCount(word: String,count: Long)
}

