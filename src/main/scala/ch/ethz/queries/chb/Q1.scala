package ch.ethz.queries.chb

import ch.ethz.queries.ChQuery
import ch.ethz.tell.TellContext
import org.apache.spark.sql.DataFrame

/**
 * Ch Query1
 *
 * select ol_number, sum(ol_quantity) as sum_qty, sum(ol_amount) as sum_amount,
 * avg(ol_quantity) as avg_qty, avg(ol_amount) as avg_amount, count(*) as count_order
 * from orderline where ol_delivery_d > '2007-01-02 00:00:00.000000'
 * group by ol_number order by ol_number
 */
class Q1 extends ChQuery {

  override def executeQuery(context: TellContext): DataFrame = {
    import org.apache.spark.sql.functions._

    val orderline = context.read.format("tell").options(Map(
      "table" -> "order-line",
      "numPartitions" -> "8"
    )).load()

    orderline
      .filter(orderline("ol_delivery_d") > referenceDate2007)
      .groupBy("ol_number")
      .agg(
        sum("ol_quantity").as("sum_qty"),
        sum("ol_amount").as("sum_amount"),
        avg("ol_quantity").as("avg_qty"),
        avg("ol_amount").as("avg_amount"),
        count("ol_number").as("count_order")
      ).sort("ol_number")
  }
}
