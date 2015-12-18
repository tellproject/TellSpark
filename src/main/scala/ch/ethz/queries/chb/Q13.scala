package ch.ethz.queries.chb

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell.PredicateType.ShortType
import ch.ethz.tell.{BufferType, CNFClause, ScanQuery, TSparkContext}
import org.apache.spark.sql.SQLContext

/**
 * select	 c_count, count(*) as custdist
from	 (select c_id, count(o_id)
	 from customer left outer join orders on (
		c_w_id = o_w_id
		and c_d_id = o_d_id
		and c_id = o_c_id
		and o_carrier_id > 8)
	 group by c_id) as c_orders (c_id, c_count)
group by c_count
order by custdist desc, c_count desc
 */
class Q13 extends ChQuery {

  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import BufferType._

    // prepare date selection
    val oSchema = ChTSchema.orderSch
    val ordQry = getOrderQry(tSparkContext)
    val oCarrierIndex = oSchema.getField("o_carrier_id").index

    val carrierSelection = new CNFClause
    carrierSelection.addPredicate(
      ScanQuery.CmpType.GREATER, oCarrierIndex, new ShortType((8).asInstanceOf[Short]))
//    ordQry.addSelection(carrierSelection)

    val cusQry = new TScanQuery("customer", tSparkContext.partNum.value, Big)
    val customer = customerRdd(tSparkContext, cusQry, ChTSchema.customerSch).toDF()

    val forders = orderRdd(tSparkContext, ordQry, oSchema).toDF()
      .filter($"o_carrier_id" > 8)

    val c_orders = customer.join(forders, $"c_w_id" === forders("o_w_id") &&
      $"c_d_id" === forders("o_d_id") &&
      $"c_id" === forders("o_c_id"), "left_outer")
      .select($"c_id", $"o_id")
      .groupBy($"c_id")
    .agg(count("o_id").as("c_count"))

    val res = c_orders
      .groupBy("c_count")
      .agg(count("c_count").as("custdist"))
      .orderBy($"custdist".desc, $"c_count".desc)

    timeCollect(res, 13)
  }

}
