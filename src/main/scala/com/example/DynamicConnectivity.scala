package com.example

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object DynamicConnectivity {

  var parent = ArrayBuffer[Int]() //parent if i
  var size = ArrayBuffer[Int]() //number of objects in subtree rooted at i
  var count = 0 // number of components

  //Initialises an empty union-find data structure with N isolated components 0 through N-1
  def WeightedQuickUnionPathCompressionUF(N: Int): Unit ={
    parent = ArrayBuffer.fill(N){0}// initialised to 0
    size = ArrayBuffer.fill(N){1} //initialised to 1
    count = N //initialised to the number of objects (tuples) in Array

    for(i <- 0 until N){
        parent(i) = i //Set parent to component id
      }
    }

  //Returns the number of components
  def Count(): Int ={
    return count
  }


  //Are two sites p and q in the same component
  def Connected(p:Int, q:Int): Boolean ={
    return Find(p) == Find(q)
  }


  //Returns the component identifier for the components containing site
  def Find(p:Int): Int ={
    Validate(p)
    var oldP = p
    var root = oldP
    while(root != parent(root)){
      root = parent(root)
    }
    while(oldP != root){
      val newP = parent(oldP)
      parent(oldP) = root
      oldP = newP
    }
    return root
  }


  //Validate that p is a valid index
  def Validate(p: Int): Unit ={
    val N = parent.length
    if(p < 0 || p >= N){
      println("index out of bounds")
    }
  }

  //Array((4,3), (3,8), (6,5), (9,4), (2,1), (8,9), (5,0), (7,2), (6,1), (1,0), (6,7))
  //Merges the component containing site p with the component containing site q
  def Union(p: Int, q: Int): Unit ={
    val rootP = Find(p)
    val rootQ = Find(q)

    if(rootP == rootQ){return}

    //Make smaller root point to larger one
    if(size(rootP) < size(rootQ)){
      parent(rootP) = rootQ
      size(rootQ) = size(rootQ) + size(rootP)
    }
    else{
      parent(rootQ) = rootP
      size(rootP) = size(rootQ) + size(rootP)
    }
    count-= 1
  }


  /*Reads in a sequence of pairs of integers (between 0 and N-1) from standard input
  where each integer represents some object
  if the objects are in different components merge the two components
   */

  def main(args: Array[String]): Unit = {
    val pairs = Array((4,3), (3,8), (6,5), (9,4), (2,1), (8,9), (5,0), (7,2), (6,1), (1,0), (6,7))
    val distVals = 10


    WeightedQuickUnionPathCompressionUF(distVals)

    for(i <- 0 until pairs.length){
      val p = pairs(i)._1
      val q = pairs(i)._2

      if(!Connected(p, q)){
        Union(p, q)
        println(p,q)
      }
    }
    println(count + " components")
  }
}
