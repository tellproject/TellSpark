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

    val oSchema = ChTSchema.orderLineSch

    // prepare date selection
    val dateSelection = new CNFClause
    dateSelection.addPredicate(
      ScanQuery.CmpType.GREATER, oSchema.getField("ol_delivery_d").index, referenceDate2007)
    val orderLineQuery = new ScanQuery
    orderLineQuery.addSelection(dateSelection)

    val orderline = orderLineRdd(scc, orderLineQuery, ChTSchema.orderLineSch).toDF()
    logger.info("[Query %d] %s".format(1, orderline.printSchema))

    //ToDo projection push downs
    val res = orderline
      .groupBy($"OL_NUMBER")
      .agg(sum($"OL_AMOUNT"),
        sum($"OL_QUANTITY"),
        avg($"OL_QUANTITY"),
        avg($"OL_AMOUNT"),
        count($"OL_NUMBER"))
      .sort($"OL_NUMBER")

    timeCollect(res, 1)
  }
}
