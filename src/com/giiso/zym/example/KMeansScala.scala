package com.giiso.zym.example

import org.apache.spark.SparkContext
import org.apache.spark._

object KMeansScala {
  
  def main(args:Array[String]){
    if(args.length < 2){
      println("Please input <dataFile> <knumbers> <epsilon>")
      return
    }
    
    //val master = args(0)
    val dataFile = args(0)
    val knumbers = args(1).toInt
    val epsilon = args(2).toDouble
    
    val conf = new SparkConf()    
    val sc = new SparkContext(conf)
    // read the data from file
    val lines = sc.textFile(dataFile)
    
    val points = lines.map ( line => {
        val parts = line.split("\t").map(_.toDouble)
          new Point(parts(0) , parts(1))
    } ).cache()
    
    //初始化K个质心
    val centroids = Array.fill(knumbers) {Point.random}
    println("test points: ")
    points.foreach(println(_))
    println("initialuize centroids: \n" + centroids.mkString("\n") + "\n")
    
    val startTime = System.currentTimeMillis()
    
    val resultCentroids = kmeans(points,centroids,epsilon,sc)
    
    val endTime = System.currentTimeMillis()
    val runTime = endTime - startTime
    println("run Time: " + runTime + "\nFinal centroids: \n"+ resultCentroids.mkString("\n"))
    
  }
  
  
  def kmeans(points: rdd.RDD[Point], centroids: Seq[Point], epsilon: Double, sc: SparkContext): Seq[Point]={
    def closestCentroid(point: Point) = {
      centroids.reduceLeft((a,b) => if ((point distance a) < (point distance b)) a else b)
    }
    var cluster = points.map ( point => (closestCentroid(point) -> (point,1)) )
    // cluster.foreach(println(_))
    
    var clusterSum = cluster.reduceByKey{ case ((ptA, numA),(ptB, numB)) => (ptA + ptB, numA + numB)}
    var average = clusterSum.map {pair => (pair._1,pair._2._1/pair._2._2)}.collectAsMap()
    
    val newCentroids = centroids.map(oldCentroid => {
      average.get(oldCentroid) match{
        case Some(newCentroid) => newCentroid
        case None => oldCentroid
      }
    })
    
    val movement = (centroids, newCentroids).zipped.map(_ distance _)
    println("Centroids changed by \n" + movement.map(d => "%sf".format(d)).mkString("(",",",")") 
    + "\nto\n" + newCentroids.mkString(",") + "\n")
    
    if (movement.exists(_>epsilon))
      kmeans(points,newCentroids,epsilon,sc)
    else 
      newCentroids
    
  }
  
}