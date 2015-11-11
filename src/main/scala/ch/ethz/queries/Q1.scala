package ch.ethz.queries

import ch.ethz.TellClientFactory
import ch.ethz.tell.{TSparkContext, TRecord, TRDD, ScanQuery}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Query1
 * select ol_number, sum(ol_quantity) as sum_qty, sum(ol_amount) as sum_amount,
 * avg(ol_quantity) as avg_qty, avg(ol_amount) as avg_amount, count(*) as count_order
 * from orderline where ol_delivery_d > '2007-01-02 00:00:00.000000'
 * group by ol_number order by ol_number
 */
object Q1 {

//  val conf = new SparkConf()
//  val sc = new SparkContext(conf)
  def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String, appName:String): Unit = {
    val scc = new TSparkContext(mUrl, appName, st, cm, cn, cs)
    println("[TELL] PARAMETERS USED: " + TellClientFactory.toString())
    //val tellRdd = new TellRDD[TellRecord](sc, "order", new ScanQuery(), CHSchema.orderLineSch)
    val orderRdd = new TRDD[TRecord](scc, "order", new ScanQuery(), CHSchema.orderLineSch)

    val grouped = orderRdd.filter(record => record.getValue("OL_DELIVERY_D").asInstanceOf[String] > "2007")
      .groupBy(record => record.getValue("OL_NUMBER").asInstanceOf[Int]).sortByKey()
      .map( p => {
      val olNumber = p._1
      val it = p._2.iterator
      var s1 = 0
      var s2 = 0
      var cnt = 0
      while(it.hasNext) {
        val record = it.next()
        s1 += record.getValue("OL_QUANTITY").asInstanceOf[Int]
        s2 += record.getValue("OL_AMOUNT").asInstanceOf[Int]
        cnt += 1
      }
      (olNumber, s1, s2, s1/cnt, s2/cnt)
    })
    grouped.collect()
    //    println("[TUPLES] %d".format(result.length))
  }

  def main(args : Array[String]) {
    var st = "192.168.0.11:7241"
    var cm = "192.168.0.11:7242"
    var cn = 4
    var cs = 5120000
    var masterUrl = "local[12]"
    var appName = "ch_Qry1"

    // client properties
    if (args.length >= 4) {
      st = args(0)
      cm = args(1)
      cn = args(2).toInt
      cs = args(3).toInt
      if (args.length == 6) {
        masterUrl = args(4)
        appName = args(5)
      } else {
        println("[TELL] Incorrect number of parameters")
        println("[TELL] <strMng> <commitMng> <chunkNum> <chunkSz> <masterUrl> <appName>")
        sys.exit()
      }
    }
    execute(st, cm, cn, cs, masterUrl, appName)
  }
}
