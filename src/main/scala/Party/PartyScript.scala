import scala.collection.mutable.ArrayBuffer


var set_new_ecid = ArrayBuffer[Array[(String, Long, String)]]()

def compare(cOld: Array[Row], cNew: Array[(String, Long, String)]: Unit = {

  val cNew_filtered = cNew.filter(x => x._2 == -1) // identify pairs with no previous ECID

  if (cNew.intersect(cOld).isEmpty && cNew_filtered.size != cNew.size) {return} // do not compare clusters that have no set relations
  else {

    if (cNew.diff(cNew_filtered).size == cOld.size && cNew.diff(cNew_filtered).diff(cOld).isEmpty) { //if two clusters completely overlap or pairs with no previous ECID were added to the historic cluster...
      val ecId = cOld(0)._2
        set_new_ecid += cNew.map(x => (x._1, ecId, x._3)) //..assign the historic ECID ...
    }

    else {
//      set_new_ecid += cNew.map(x => (x._1, -1L, x._3)) //... else generate a new ECID
      set_new_ecid += cNew.map(x => (x(0), {
        val i = scala.math.abs(new scala.collection.immutable.StringOps(x(0)).hashCode()).toLong
        if (i % 3 == 0) {
          -1
        } else { i }
      }, x(2))) //... else generate a new ECID
    }
  }
}


case class cmp(uuid: String, ecid: Long, id: String)

val set_old = sqlContext.parquetFile("/data/cfs/dev01/work/custm/components-1").map(row => cmp(row(0).asInstanceOf[String], row(1).asInstanceOf[Long], row(2).asInstanceOf[String])).groupBy(x => x.uuid).map(x => x._2)


  groupBy(x => x(0)).map(
  x => x._2)
val set_new = sqlContext.parquetFile("/data/cfs/dev01/work/custm/components-1").collect.groupBy(x => x(0)).map(
  x => x._2)


//var outerloop: Int = 1
//var innerloop: Int = 1
//
//
//if(set_new.size > set_old.size){
//  outerloop = set_old.size
//  innerloop = set_new.size
//} else {
//  outerloop = set_new.size
//  innerloop = set_old.size
//}
//
//
//for (i <- 0 until outerloop) {
//  for (j <- 0 until innerloop) {
//   compare(set_old(i), set_new(j))
//   }
//}

if(set_new.size > set_old.size){
  for(i <- set_old) {
    for (j <- set_new){
      compare(i, j)
    }
  }
} else {
  for (i <- set_new){
    for(j <- set_old){
      compare(j, i)
    }
  }
}



sc.parallellize(set_new_ecid.distinct).map(x => {cmp(x._1, x._2, x._3}).toDF.saveAsParquetFile("/data/cfs/dev01/work/custm/components")



