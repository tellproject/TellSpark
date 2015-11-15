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
    println("===========")
    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // convert an RDDs to a DataFrames
    val oo = new TRDD[TRecord](scc, "order-line", new ScanQuery(), ChTSchema.orderLineSch)
    val ol = oo.map(r => {
      OrderLine(r.getValue("OL_O_ID").asInstanceOf[Int],
        r.getValue("OL_D_ID").asInstanceOf[Short],
        r.getValue("OL_W_ID").asInstanceOf[Int],
        r.getValue("OL_NUMBER").asInstanceOf[Short],
        r.getValue("OL_I_ID").asInstanceOf[Int],
        r.getValue("OL_SUPPLY_W_ID").asInstanceOf[Int],
        r.getValue("OL_DELIVERY_D").asInstanceOf[Long],
        r.getValue("OL_QUANTITY").asInstanceOf[Short],
        r.getValue("OL_AMOUNT").asInstanceOf[Long],
        r.getValue("OL_DIST_INFO").asInstanceOf[String]
      )
    })
    val orderline = ol.toDF()
    println("%%%%%%%%%%%%%%%%%%^^^^^^^^^^^^^^^^^^")
    println("^^^^^^^^^^^^^^^^^^%%%%%%%%%%%%%%%%%%")
    println("==============================" + oo.count)
    ol.map(r => println(r.toString))
    ol.collect()
    println("////////////////%%%%%%%%%%%%%%%%%%")
    println("%%%%%%%%%%%%%%%%%%///////////////")
    //Do push downs
      val res = orderline.filter($"OL_DELIVERY_D" >= "1999-01-01")
        .filter($"OL_DELIVERY_D" < "2020-01-01")
        .filter($"OL_QUANTITY" >= "1").filter($"OL_QUANTITY" <= "10000")
        .agg(sum($"OL_AMOUNT"))
      //outputDF(res)
  }
}
