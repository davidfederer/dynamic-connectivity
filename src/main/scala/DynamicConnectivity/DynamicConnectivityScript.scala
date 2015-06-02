/**
 * Created by david on 26/05/15.
 */

import java.util.UUID
import scala.collection.mutable

val parent = mutable.HashMap[String, String]()  //parent if i
val size = mutable.HashMap[String, Int]()  //number of objects in subtree rooted at i
val cluster = mutable.HashMap[String, String]() //private val component = mutable.HashMap[String, String]()
val ecid_map = mutable.HashMap[String, Long]() //creates key value pairs for client ids and ecids


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

//Are two sites p and q in the same component
def connected(p: String, q: String): Boolean = {
  return find(p).equals(find(q))
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
}

def checkIn(site: String, ecid: Long): Unit = {
  if(!parent.contains(site)) {
    parent(site) = site
    size(site) = 1
    cluster(site) = UUID.randomUUID().toString
    ecid_map(site) = ecid
  }
}

for (i <- sqlContext.parquetFile("/data/cfs/dev01/work/custm/pow1_full_parquet-1").collect) {
  val p = i(2).asInstanceOf[String]
  val q = i(3).asInstanceOf[String]
  val r = i(1).asInstanceOf[Long]

  checkIn(p, r)
  checkIn(q, r)

  if (!connected(p, q)) {
    union(p, q)
  }
}


val p = parent.keys.map(
  x => {(cluster(find(x)), {if (ecid_map(x) > -1) ecid_map(x) else -1}, x)}
).toSeq


case class cmp(uuid: String, ecid: Long, id: String)
sc.parallelize(p).map(x => {cmp(x._1, x._2, x._3)}).toDF.saveAsParquetFile("/data/cfs/dev01/work/custm/components-1")






