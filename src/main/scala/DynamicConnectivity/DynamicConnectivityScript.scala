/**
 * Created by david on 26/05/15.
 */


import java.util.UUID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.{StructType, StructField, StringType};

val parent = mutable.HashMap[String, String]()  //parent if i
val size = mutable.HashMap[String, Int]()  //number of objects in subtree rooted at i
val cluster = mutable.HashMap[String, String]() //private val component = mutable.HashMap[String, String]()
var count = 0 // number of components
val ecid_map = mutable.HashMap[String, String]() //creates key value pairs for client ids and ecids


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


val new_pairs = sqlContext.sql("SELECT id0, id1, ecid FROM lighthouse_sandpit.customermatching_pow1_orc WHERE match_score >= 0.8").map(
  row => (row(0).asInstanceOf[String], row(1).asInstanceOf[String], row(2).asInstanceOf[String])
)

for (i <- new_pairs.toArray) {
  val p = i._1
  val q = i._2
  val r = i._3

  checkIn(p, r)
  checkIn(q, r)

  if (!connected(p, q)) {
    union(p, q)
  }
}


//val p = ArrayBuffer[(String, String, String)]()
//
//for (i <- parent.keys) {
//  val t = (cluster(find(i)), cid_map(i), i)
//  p += t
//}


val p = parent.keys.map(
  x => {(cluster(find(x)), {if (!ecid_map(x).isEmpty) ecid_map(x) else ""}, x)}
)


case class cmp(uuid: String, ecid: String, id: String)
sc.parallellize(p).map(x => {cmp(x._1, x._2, x._3)}).toDF.saveAsParquet("/data/cfs/dev01/work/custm/components")


//println("number of clusters: " + count)


def checkIn(site: String, ecid: String): Unit = {
  if(!parent.contains(site)) {
    parent(site) = site
    size(site) = 1
    cluster(site) = UUID.randomUUID().toString
    count = count + 1
    ecid_map(site) = ecid
  }
}



