package ch.ethz

import org.apache.spark.{SparkConf, SparkContext}
import ch.ethz.tell._
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

/**
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

    TellClientFactory.storageMng = st
    TellClientFactory.commitMng = cm
    TellClientFactory.chNumber = cn
    TellClientFactory.chSize = cs

    println("[TELL] PARAMETERS USED: " + TellClientFactory.toString())

    // schema to be read
    val sch: TellSchema = new TellSchema()

    sch.addField(Schema.FieldType.INT, "number", true)
    sch.addField(Schema.FieldType.TEXT, "text1", true)
    sch.addField(Schema.FieldType.BIGINT, "largenumber", true)
    sch.addField(Schema.FieldType.TEXT, "text2", true)

    val tblName = "testTable"
    // rdd creation
//    val tellRdd = new TellRDD[TellRecord](sc, tblName, new ScanQuery(), sch)
    val tellRdd = new TRDD[Customer](sc, tblName, new ScanQuery(), null)

    println("=============MAPPING==============")
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
