package com.learning

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object HelloWorld {

  def main(args: Array[String]): Unit = {
    //获取flink的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val dataStream=env.socketTextStream("node01", 9999)
//    dataStream.flatMap(new FlatMapFunction(){
//
//    })

    println("hello world")



  }


}
