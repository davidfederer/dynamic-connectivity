import scala.collection.mutable.ArrayBuffer


var set_new_ecid = ArrayBuffer[List[(String, Long, String)]]()

def compare(cOld: List[(String, Long, String)], cNew: List[(String, Long, String)]): Unit = {

  val cNew_filtered = cNew.filter(x => x._2 == -1) // identify pairs with no previous ECID

  if (cNew.intersect(cOld).isEmpty && cNew_filtered.size != cNew.size) {return} // do not compare clusters that have no set relations
  else {

    if (cNew.diff(cNew_filtered).size == cOld.size && cNew.diff(cNew_filtered).diff(cOld).isEmpty) { //if two clusters completely overlap or pairs with no previous ECID were added to the historic cluster...
      val ecId = cOld(0)._2
        set_new_ecid += cNew.map(x => (x._1, ecId, x._3)) //..assign the historic ECID ...
    }

    else {
      set_new_ecid += cNew.map(x => (x._1, -1.toLong, x._3)) //... else generate a new ECID
    }
  }
}



val set_old = sqlContext.parquetFile("/data/cfs/dev01/work/custm/components").groupBy(x => x._1).map(
  x => x._2)
val set_new = sqlContext.parquetFile("/data/cfs/dev01/work/custm/components").groupBy(x => x._1).map(
  x => x._2)


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
   compare(set_old(i), set_new(j))
   }
}


case class cmp(uuid: String, ecid: String, id: String)
sc.parallellize(set_new_ecid.distinct).map(x => {cmp(x._1, x._2, x._3}).toDF.saveAsParquetFile("/data/cfs/dev01/work/custm/components")



