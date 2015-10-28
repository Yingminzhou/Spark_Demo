package com.giiso.zym.ChineseAnalyzer


import collection.mutable.ArrayBuffer
import scala.io.Source

object ForwardMatch {
  def main(args: Array[String]){
    val wordLists = Source.fromFile("dict.txt","utf-8").getLines.toArray
    val wordList = segmentation(wordLists,15,"如果是txt的词库，就是用空格分隔开的所有词都是词库。如果是数据库，就是往里面加一条记录")
    wordList.foreach(s => print(s + '\n'))
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
          if ((strList.contains(word))|| word.length()==1 ){
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
