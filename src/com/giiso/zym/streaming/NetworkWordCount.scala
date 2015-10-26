package com.giiso.zym.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

object NetworkWordCount {
   def main(args:Array[String]){
    if(args.length < 2){
      println("Usage: <ip> <port> <seconds>")
      return
    }
    val ip = args(0)
    val port = args(1).toInt
    val seconds = args(2).toInt
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(seconds));
    // 获得一个DStream负责连接 监听端口:地址
    val lines = ssc.socketTextStream(ip, port);
    // 对每一行数据执行Split操作
    val words = lines.flatMap(_.split(" "));
    // 统计word的数量
    val pairs = words.map(word => (word, 1));
    val wordCounts = pairs.reduceByKey(_ + _);
    // 输出结果
    wordCounts.print();
    ssc.start();             // 开始
    ssc.awaitTermination();  // 计算完毕退出
   }
}