package ch.ethz.queries

import ch.ethz.tell._

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

    val sQry = new ScanQuery()
    val cl1 = new sQry.CNFCLause
    val ff = new PredicateType.FloatType(10)
    cl1.addPredicate(ScanQuery.CmpType.EQUAL, 1, ff)
    // convert an RDDs to a DataFrames
    val oo = new TRDD[TRecord](scc, "order-line", sQry, ChTSchema.orderLineSch).map(r => {
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
    println("======== Q1 ===============")
    val olc = oo.count()
    println("======== Q1 ===============" + olc )
    val orderline = oo.toDF()
    println("======== Q1 ===============")
    val oll = orderline.count()
    println("======== Q1 ===============" + oll)

    //ToDo push downs
 //   val res = orderline.filter($"OL_DELIVERY_D" > "2007-01-02")
     // .groupBy($"OL_NUMBER")
   //   .agg(sum($"OL_AMOUNT"),
  //      sum($"OL_QUANTITY"),
  //      avg($"OL_QUANTITY"),
  //      avg($"OL_AMOUNT"),
  //      count($"OL_NUMBER"))
  //    .sort($"OL_NUMBER")

    //timeCollect(res, 1)
  }
}
