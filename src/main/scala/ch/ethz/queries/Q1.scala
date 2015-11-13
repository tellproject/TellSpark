package ch.ethz.queries

import ch.ethz.tell.{TSparkContext, TRecord, TRDD, ScanQuery}

/**
 * Query1
 * select ol_number, sum(ol_quantity) as sum_qty, sum(ol_amount) as sum_amount,
 * avg(ol_quantity) as avg_qty, avg(ol_amount) as avg_amount, count(*) as count_order
 * from orderline where ol_delivery_d > '2007-01-02 00:00:00.000000'
 * group by ol_number order by ol_number
 */
class Q1 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // convert an RDDs to a DataFrames
    val orderline = new TRDD[TRecord](scc, "orderline", new ScanQuery(), ChTSchema.orderLineSch).map(r => {
      OrderLine(r.getField("OL_O_ID").asInstanceOf[Int],
        r.getField("OL_D_ID").asInstanceOf[Short],
        r.getField("OL_W_ID").asInstanceOf[Int],
        r.getField("OL_NUMBER").asInstanceOf[Short],
        r.getField("OL_I_ID").asInstanceOf[Int],
        r.getField("OL_SUPPLY_W_ID").asInstanceOf[Int],
        r.getField("OL_DELIVERY_D").asInstanceOf[Long],
        r.getField("OL_QUANTITY").asInstanceOf[Short],
        r.getField("OL_AMOUNT").asInstanceOf[Double],
        r.getField("OL_DIST_INFO").asInstanceOf[String]
      )
    }).toDF()

    //Do push downs
    val res = orderline.filter($"OL_DELIVERY_D" > "2007-01-02")
      .groupBy($"OL_NUMBER")
      .agg(sum($"OL_AMOUNT"),
        sum($"OL_QUANTITY"),
        avg($"OL_QUANTITY"),
        avg($"OL_AMOUNT"),
        count($"OL_NUMBER"))
      .sort($"OL_NUMBER")
    //outputDF(res)
  }
}