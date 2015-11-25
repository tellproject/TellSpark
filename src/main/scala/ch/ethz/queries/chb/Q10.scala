package ch.ethz.queries.chb

import ch.ethz.queries.ChQuery
import ch.ethz.tell.{CNFClause, ScanQuery, TSparkContext}

/**
 * select	 c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
from	 customer, orders, orderline, nation
where	 c_id = o_c_id
	 and c_w_id = o_w_id
	 and c_d_id = o_d_id

	 and ol_w_id = o_w_id
	 and ol_d_id = o_d_id
	 and ol_o_id = o_id
 and o_entry_d <= ol_delivery_d

	 and o_entry_d >= '2007-01-02 00:00:00.000000'
	 and n_nationkey = ascii(substr(c_state,1,1))
group by c_id, c_last, c_city, c_phone, n_name
order by revenue desc
 */
class Q10  extends ChQuery {

  override def execute(st: String, cm: String, cn:Int, cs:Long, mUrl:String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // prepare date selection
    val oSchema = ChTSchema.orderSch
    val dateSelection = new CNFClause
    dateSelection.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oSchema.getField("o_entry_d").index, referenceDate2007)
    val orderQuery = new ScanQuery
//    orderQuery.addSelection(dateSelection)

    val cc = customerRdd(scc, new ScanQuery, ChTSchema.customerSch)
    val oo = orderRdd(scc, orderQuery, oSchema)
    val ol = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch)
    val nn = nationRdd(scc, new ScanQuery, ChTSchema.nationSch)

    val orderline = ol.toDF()
    val customer = cc.toDF()
    val nation = nn.toDF()
    val forders = oo.toDF()
    //      .filter($"o_entry_d" >= 20070102)
    val c_n = customer.join(nation, $"c_state".substr(1,1) === nation("n_nationkey"))
    val o_ol = forders.join(orderline, (orderline("ol_w_id") === $"o_w_id" &&
      orderline("ol_d_id") === $"o_d_id" &&
      orderline("ol_o_id") === $"o_id" &&
      orderline("ol_delivery_d") >= $"o_entry_d" ) )
    val res = c_n.join(o_ol, ( (c_n("c_id") === o_ol("o_c_id")) &&
      (c_n("c_w_id") === o_ol("o_w_id")) &&
      (c_n("c_d_id") === o_ol("o_d_id")) ))
      //c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
    .select("c_id", "c_last", "c_city", "c_phone", "n_name", "ol_amount")
    .agg(sum($"ol_amount").as("revenue"))
    .orderBy($"revenue".desc)

    timeCollect(res, 10)
    scc.sparkContext.stop()
  }

}
