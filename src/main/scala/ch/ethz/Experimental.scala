package ch.ethz

import org.apache.spark.{SparkConf, SparkContext}
import ch.ethz.tell._

/**
 * Sanity checking class
 */
object Experimental {

  val conf = new SparkConf().setMaster("local[1]")
  val sc = new SparkContext(conf)

  def main(args : Array[String]) {
    var st = "192.168.0.21:7241"
    var cm = "192.168.0.21:7242"
    var cn = 4
    var cs = 5120000

    // client properties
    if (args.length == 4) {
      st = args(0)
      cm = args(1)
      cn = args(2).toInt
      cs = args(3).toInt
    }

    TClientFactory.storageMng = st
    TClientFactory.commitMng = cm
    TClientFactory.chNumber = cn
    TClientFactory.chSize = cs

    println("[TELL] PARAMETERS USED: " + TClientFactory.toString())

    val tblName = "testTable"

    // schema to be read
    TClientFactory.startTransaction()
    val sch: TSchema = new TSchema(TClientFactory.trx.schemaForTable(tblName))

    // rdd creation
    // readLine()
    val scc = new TSparkContext("local[1]", "Experimental", st, cm, cn, cs)
    val tellRdd = new TRDD[TRecord](scc, tblName, new ScanQuery(), sch)

//    val grouped = tellRdd.filter(record => record.getField() > "2007")
//      .groupBy(record => record.getcId()).sortByKey().map( p => {
//      val idd = p._1
//      val it = p._2.iterator
//      var s1 = 0
//      var s2 = 0
//      var cnt = 0
//      while(it.hasNext) {
//       val cus = it.next()
//        s1 += cus.getdId()
//        s2 += cus.getwId()
//        cnt += 1
//      }
//      (idd, s1, s2, s1/cnt, s2/cnt)
//    })

    println("=============COLLECTING==============")
    val result = tellRdd.collect()
    println("=====================================")
    println("[TUPLES] %d".format(result.length))
    println("======================================")
    println("======================================")
  }

}
