package com.giiso.zym.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
 
object FileWordCount {
  def main(args: Array[String]) {
     if(args.length < 2){
      println("Usage: <seconds> <port>")
      return
    }
    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
 
    // 创建Streaming的上下文，包括Spark的配置和时间间隔，这里时间为间隔20秒
    val ssc = new StreamingContext(sparkConf, Seconds(args(0).toInt))
 
    // 指定监控的目录，在这里为/home/hadoop/temp/
    val lines = ssc.textFileStream(args(1))
 
    // 对指定文件夹变化的数据进行单词统计并且打印
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
 
       // 启动Streaming
    ssc.start()
    ssc.awaitTermination()
  }
}