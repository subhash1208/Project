package com.adp.datacloud.ds.udafs

object myTest {
  
  def main(args: Array[String]) {
    val myArray = Array(10,20,30)
    val newArray = myArray map { x =>
      println(x);
      x*x
    }
    println("Done");
    
  }
  
}