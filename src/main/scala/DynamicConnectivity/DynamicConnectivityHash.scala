package com.example

//import scala.collection.mutable.ArrayBuffer
//import scala.io.Source

import java.util.UUID

import scala.collection.mutable

object DynamicConnectivityHash {

  //var parent = mutable.HashMap[String, mutable.Map[String, String]]()//parent if i
  //var size = mutable.HashMap[String, mutable.Map[String, Int]]() //number of objects in subtree rooted at i

  var parent = mutable.HashMap[String, String]()//parent if i
  var size = mutable.HashMap[String, Int]() //number of objects in subtree rooted at i
  var cluster = mutable.HashMap[String, String]()
  var cluster_id = List[Tuple2[String, String]]()
  var count = 0 // number of components

  //Initialises an empty union-find data structure with N isolated components 0 through N-1
  def weightedQuickUnionPathCompressionUF(distVal: List[String]): Unit = {

    for (i <- distVal) {

      parent(i) = i
      size(i) = 1
      cluster(i) = UUID.randomUUID().toString

      //parent(UUID.randomUUID().toString) = mutable.Map(i -> i) //parent.put(UUID.randomUUID().toString, Map(i -> i))
      //size(UUID.randomUUID().toString) = mutable.Map(i -> 1)
    }

    count = distVal.length //initialised to the number of objects (tuples) in Array
  }

  //Returns the number of components
  def count_component(): Int ={
    return count
  }


  //Are two sites p and q in the same component
  def connected(p:String, q:String): Boolean ={
    return find(p).equals(find(q))
  }


  //Returns the component identifier for the components containing site
  def find(p:String): String ={
    var oldP = p
    var root = oldP
    while(!root.equals(parent(root))){
      root = parent(root)
    }
    while(!oldP.equals(root)){
      val newP = parent(oldP)
      parent(oldP) = root
      oldP = newP
    }
    return root
  }


  //Array((4,3), (3,8), (6,5), (9,4), (2,1), (8,9), (5,0), (7,2), (6,1), (1,0), (6,7))
  //Merges the component containing site p with the component containing site q
  def union(p: String, q: String): Unit ={
    val rootP = find(p)
    val rootQ = find(q)

    if(rootP.equals(rootQ)){return}

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
    val pairs = List(List("ct456", "ct123"), List("ct567","ct456"), List("ct567","ct478"), List("ct789", "cp345"), List("cp856", "cp678"))
    val distVals = pairs.flatten.distinct


    weightedQuickUnionPathCompressionUF(distVals)

    for(i <- 0 until pairs.length){
      val p = pairs(i)(0)
      val q = pairs(i)(1)

      if(!connected(p, q)){
        union(p, q)
        println(p,q)
      }
    }

    for(i <- distVals){
      cluster_id ::= (cluster(find(i)), i)
    }
    println("cluster ids: "+cluster_id)

    println("number of clusters: "+count)
  }
}
