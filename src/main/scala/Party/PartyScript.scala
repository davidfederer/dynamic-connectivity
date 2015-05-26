import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable

var set_new_ecid = ArrayBuffer[List[(String, String)]]()

def compare(cOld: List[(String, String)], cNew: List[(String, String)]): Unit = {

  val cNew_filtered = cNew.filter(x => x._1 == "") // identify pairs with no previous ECID

  if (cNew.intersect(cOld).isEmpty && cNew_filtered.size != cNew.size) {return} // do not compare clusters that have not set relations
  else {

    if (cNew.diff(cNew_filtered).size == cOld.size && cNew.diff(cNew_filtered).diff(cOld).isEmpty) { //if two clusters completely overlap or pairs with no previous ECID were added to the historic cluster...
      val ecId = cOld(0)._1
        set_new_ecid += cNew.map(x => (ecId, x._2)) //..assign the historic ECID ...
    }

    else {
      set_new_ecid += cNew.map(x => ("new ECID", x._2)) //... else generate a new ECID
    }
  }
}

    //historic set of clusters
    val cluster_old = List(("d2d9d98b-1730-4460-9fbd-94c4d485bf12", "94c4d485bf12", "cp678"),
      ("d2d9d98b-1730-4460-9fbd-94c4d485bf12", "94c4d485bf12", "cp856"),
      ("85d2700f-a6c1-4fed-9b13-bca8955fcfcc", "bca8955fcfcc", "ct789"),
      ("e870d741-b70e-4001-98e6-f24862ba60f1", "f24862ba60f1", "ct567"),
      ("e870d741-b70e-4001-98e6-f24862ba60f1", "f24862ba60f1", "ct478"),
      ("e870d741-b70e-4001-98e6-f24862ba60f1", "f24862ba60f1", "ct456"),
      ("e870d741-b70e-4001-98e6-f24862ba60f1", "f24862ba60f1", "ct123"))

    //new set of clusters
    val cluster_new = List(("d2d9d98b-1730-4460-9fbd-94c4d485bf12", "94c4d485bf12", "cp678"), //SCENARIO 1 (m/b rules 1, 8)
      ("d2d9d98b-1730-4460-9fbd-94c4d485bf12", "94c4d485bf12", "cp856"),
      ("d2d9d98b-1730-4460-9fbd-94c4d485bf11", "", "cp350"), // SCENARIO 2 (m/b rules 2, 5)
      ("85d2700f-a6c1-4fed-9b13-bca8955fcfcc", "", "cp345"), // SCENARIO 3 (m/b rule 3)
      ("85d2700f-a6c1-4fed-9b13-bca8955fcfcc", "bca8955fcfcc", "ct789"),
      ("e870d741-b70e-4001-98e6-f24862ba60f2", "f24862ba60f1", "ct567"), //SCENARIO 4 (m/b rules 4, 6, 7, 10)
      ("e870d741-b70e-4001-98e6-f24862ba60f2", "f24862ba60f1", "ct478"),
      ("e870d741-b70e-4001-98e6-f24862ba60f1", "f24862ba60f1", "ct456"),
      ("e870d741-b70e-4001-98e6-f24862ba60f1", "f24862ba60f1", "ct124"))

    val set_old = cluster_old.groupBy(x => x._1).map(x => { //group by cluster ID and remove UUID
      x._2.map(x => (x._2, x._3))
    }).toList

    val set_new = cluster_new.groupBy(x => x._1).map(x => { //group by cluster ID and remove UUID
      x._2.map(x => (x._2, x._3))
    }).toList


    var outerloop: Int = 1
    var innerloop: Int = 1

    if(set_new.size > set_old.size){
      outerloop = set_old.size
      innerloop = set_new.size
    }

    else {
      outerloop = set_new.size
      innerloop = set_old.size
    }

    for (i <- 0 until outerloop) {
      for (j <- 0 until innerloop) {
        //println(set_old(i), set_new(j), "(" + i, j + ")")
        compare(set_old(i), set_new(j))
      }
    }
    println(set_new_ecid.distinct)
  }
}


