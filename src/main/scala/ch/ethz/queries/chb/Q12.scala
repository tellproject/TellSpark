package ch.ethz.queries.chb

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell.{BufferType, CNFClause, ScanQuery, TSparkContext}
import org.apache.spark.sql.SQLContext

/**
 * select	 o_ol_cnt,
	 sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
	 sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
from	 orders, orderline
where	 ol_w_id = o_w_id
	 and ol_d_id = o_d_id
	 and ol_o_id = o_id
	 and o_entry_d <= ol_delivery_d
	 and ol_delivery_d < '2020-01-01 00:00:00.000000'
group by o_ol_cnt
order by o_ol_cnt
 */
class Q12  extends ChQuery {

  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import BufferType._
//
    val high_line_count = udf { (x: Int) => if (x == 1 || x == 2) 1 else 0 }
    val low_line_count = udf { (x: Int) => if (x != 1 && x != 2) 1 else 0 }

    // prepare date selection
    val oSchema = ChTSchema.orderLineSch
    val olQry = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    val oDeliveryIndex = oSchema.getField("ol_delivery_d").index

    val dateSelection = new CNFClause
    dateSelection.addPredicate(
      ScanQuery.CmpType.LESS, oDeliveryIndex, referenceDate2020First)
//    olQry .addSelection(dateSelection)

    val ordQry = new TScanQuery("order", tSparkContext.partNum.value, Big)

    val orders = orderRdd(tSparkContext, ordQry, ChTSchema.orderSch).toDF()
    val forderline = orderLineRdd(tSparkContext, olQry , oSchema).toDF()
//      .filter($"ol_delivery_d" < 20200101)

    val res = orders.join(forderline, forderline("ol_w_id") === $"o_w_id" &&
      forderline("ol_d_id") === $"o_d_id" &&
      forderline("ol_o_id") === $"o_id" &&
      $"o_entry_d" <= forderline("ol_delivery_d"))

    .select($"o_ol_cnt", $"o_carrier_id")
    .groupBy($"o_ol_cnt")
    .agg(sum(high_line_count($"o_carrier_id")), sum(low_line_count($"o_carrier_id")) )
    .orderBy($"o_ol_cnt")
//
    timeCollect(res, 12)
  }
}
