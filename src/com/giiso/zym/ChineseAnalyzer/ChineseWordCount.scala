package com.giiso.zym.ChineseAnalyzer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import collection.mutable.ArrayBuffer
import scala.io.Source
 
object ChineseWordCount {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: StatefulWordCount <filename> <port> <dict-filename> ")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
 
    val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
 
    // 创建StreamingContext，Spark Steaming运行时间间隔为5秒
    val ssc = new StreamingContext(sc, Seconds(5))
    // 定义checkpoint目录为当前目录
    ssc.checkpoint(".")
 
    // 获取从Socket发送过来数据
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val wordLists = Source.fromFile(args(2),"utf-8").getLines.toArray
    val words = lines.flatMap(segmentation(wordLists,15,_))
    val wordCounts = words.map((_,1))
 
    // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val stateDstream = wordCounts.updateStateByKey[Int](updateFunc)
    
    val topStream = stateDstream.map {
        case(key, value) => (value, key); //exchange key and value
    }.transform(_.sortByKey(false))
    topStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
  def segmentation(strList:Seq[String],maxLen:Int,stc:String): Seq[String]={
    var sentence = stc
    var wordList = new ArrayBuffer[String]()
      while(sentence.length()>0){
        var min = sentence.length()
        if ( min > maxLen) {
          min = maxLen
        }
        var word = sentence.substring(0,min)
        var meet = false
        while (!meet && word.length()>0){
          if ((strList.contains(word)) || word.length()==1 ){
            wordList += word
            sentence = sentence.substring(word.length(),sentence.length())
            meet = true
          }else{
            word = word.substring(0,word.length()-1)
          }          
        }
      }
    return wordList.filter { x => x.length()!=1 }
  }
}