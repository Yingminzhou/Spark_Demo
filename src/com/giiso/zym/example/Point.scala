package com.giiso.zym.example

object Point {
  def random() = {
    new Point(math.random*50,math.random*50)
  }
}

case class Point(val x: Double, val y:Double){
  def +(that: Point) = new Point(this.x + that.x, this.y + that.y)
  def -(that: Point) = new Point(this.x - that.x, this.y - that.y)
  def /(d: Double) = new Point(this.x / d, this.y / d)
  def pointLength = math.sqrt(x * x + y * y)
  def distance(that: Point) = (this - that).pointLength
  
  override def toString = format("(%.3f,%.3f)",x,y)
}