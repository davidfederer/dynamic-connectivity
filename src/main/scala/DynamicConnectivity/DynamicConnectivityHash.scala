package com.example

import java.util.UUID

import scala.collection.mutable

object DynamicConnectivityHash {

  private val parent = mutable.HashMap[String, String]()
  //parent if i
  private val size = mutable.HashMap[String, Int]()
  //number of objects in subtree rooted at i
  private val cluster = mutable.HashMap[String, String]()
  private var count = 0 // number of components

  //Returns the number of components
  def countComponent(): Int = {
    return count
  }

  //Are two sites p and q in the same component
  def connected(p: String, q: String): Boolean = {
    return find(p).equals(find(q))
  }


  //Returns the component identifier for the components containing site
  def find(p: String): String = {
    var oldP = p
    var root = oldP
    while (!root.equals(parent(root))) {
      root = parent(root)
    }
    while (!oldP.equals(root)) {
      val newP = parent(oldP)
      parent(oldP) = root
      oldP = newP
    }
    return root
  }

  //Merges the component containing site p with the component containing site q
  def union(p: String, q: String): Unit = {
    val rootP = find(p)
    val rootQ = find(q)

    if (rootP.equals(rootQ)) {
      return
    }

    //Make smaller root point to larger one
    if (size(rootP) < size(rootQ)) {
      parent(rootP) = rootQ
      size(rootQ) = size(rootQ) + size(rootP)
    }
    else {
      parent(rootQ) = rootP
      size(rootP) = size(rootQ) + size(rootP)
    }
    count -= 1
  }


  /*Reads in a sequence of pairs of integers (between 0 and N-1) from standard input
  where each integer represents some object
  if the objects are in different components merge the two components
   */

  def main(args: Array[String]): Unit = {
    val pairs = List(List("1", "2"), List("3", "1"), List("3", "4"), List("5", "6"), List("7", "8"))

    for (i <- 0 until pairs.length) {
      val p = pairs(i)(0)
      val q = pairs(i)(1)

      checkIn(p)
      checkIn(q)

      if (!connected(p, q)) {
        union(p, q)
        println(p, q)
      }
    }

    for (i <- parent.keys) {
      println(i + ", " + cluster(find(i)))
    }
    println("number of clusters: " + count)
  }

  def checkIn(site: String): Unit = {
    if(!parent.contains(site)) {
      parent(site) = site
      size(site) = 1
      cluster(site) = UUID.randomUUID().toString
      count = count + 1
    }
  }
}
