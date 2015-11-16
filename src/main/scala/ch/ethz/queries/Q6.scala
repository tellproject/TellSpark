package ch.ethz.queries

import ch.ethz.tell.{TSparkContext, ScanQuery, TRecord, TRDD}

/**
 * Query6
 * select	sum(ol_amount) as revenue
 * from	orderline
 * where	ol_delivery_d >= '1999-01-01 00:00:00.000000'
 * and ol_delivery_d < '2020-01-01 00:00:00.000000'
 * and ol_quantity between 1 and 100000
 */
class Q6 extends ChQuery {

  override def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // convert an RDDs to a DataFrames
//    val oo = new TRDD[TRecord](scc, "order-line", new ScanQuery(), ChTSchema.orderLineSch)
    val ol = orderLineRdd(scc, new ScanQuery())
    val orderline = ol.toDF()
    //Do push downs
      val res = orderline.filter($"ol_delivery_d" >= 19990101)
        .filter($"ol_delivery_d" < 20200101)
        .filter($"ol_quantity" >= 1).filter($"ol_quantity" <= 10000)
        .agg(sum($"ol_amount"))

    timeCollect(res, 6)
  }
}
