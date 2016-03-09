case class cmp(uuid: String, ecid: Long, id: String)

def compare(o: Iterable[cmp], n: Iterable[cmp]): Array[(String, Long, String)] = {
  val cOld = o.map(x => (x.ecid, x.id)).toArray
  val cNew = n.map(x => (x.uuid, x.ecid, x.id)).toArray
  val cNew_filtered = cNew.filter(x => x._2 == -1).map(x => (x._2,x._3)) // identify pairs with no previous ECID

  if (cNew.map(x => (x._2, x._3)).diff(cNew_filtered).length == cOld.length && cNew.map(x => (x._2, x._3)).diff(cNew_filtered).diff(cOld).isEmpty) { //if two clusters completely overlap or pairs with no previous ECID were added to the historic cluster...
  val ecId = cOld(0)._1
    return cNew.map(x => (x._1, ecId, x._3))
  }
  else {
    //        set_new_ecid += cNew.map(x => (x._1, -1L, x._3)) //... else generate a new ECID
    return cNew.map(x => (x._1,{
      val i = scala.math.abs(new scala.collection.immutable.StringOps(x._1).hashCode()).toLong
      if (i % 3 == 0) {
        -1
      } else { i }
    }
      , x._3)) //... else generate a new ECID
  }
}

def hasNoSetRelation(o: Iterable[cmp], n: Iterable[cmp]): Boolean = { // do not compare clusters that have no set relations

  val cOld = o.map(x => (x.ecid, x.id)).toArray
  val cNew = n.map(x => (x.ecid, x.id)).toArray
  val cNew_filtered = cNew.filter(x => x._1 == -1)
  return (cNew.intersect(cOld).isEmpty /* && !cNew.diff(cNew_filtered).isEmpty*/)
}

val set_old = sqlContext.parquetFile("/data/cfs/dev01/work/custm/components-1").map(
  row => cmp(row(0).asInstanceOf[String], row(1).asInstanceOf[Long], row(2).asInstanceOf[String])).groupBy(
    x => x.uuid).map(
    x => x._2).cache()

val set_new = sqlContext.parquetFile("/data/cfs/dev01/work/custm/components-1").map(
  row => cmp(row(0).asInstanceOf[String], row(1).asInstanceOf[Long], row(2).asInstanceOf[String])).groupBy(
    x => x.uuid).map(
    x => x._2).cache()

val res = set_old.cartesian(set_new).filter(x => {hasNoSetRelation(x._1, x._2) == false}).distinct.map(x => compare(x._1, x._2)).map(x => {cmp(x(0).asInstanceOf[String], x(1).asInstanceOf[Long], x(2).asInstanceOf[String])}).toDF()

res.saveAsParquetFile("/data/cfs/dev01/work/custm/components")