package ch.ethz.queries

import ch.ethz.tell.PredicateType.StringType
import ch.ethz.tell.{CNFClause, ScanQuery, TSparkContext}

/**
 * Query21
 */
class Q21 extends ChQuery {

  /**
   * select	 su_name, count(*) as numwait
   * from	 supplier, orderline l1, orders, stock, nation
   * where	 ol_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id and l1.ol_delivery_d > o_entry_d
   * and ol_w_id = s_w_id and ol_i_id = s_i_id and mod((s_w_id * s_i_id),10000) = su_suppkey
   * and not exists (
   *    select * from orderline l2
   *    where l2.ol_o_id = l1.ol_o_id and l2.ol_w_id = l1.ol_w_id
   *    and l2.ol_d_id = l1.ol_d_id and l2.ol_delivery_d > l1.ol_delivery_d
   *    )
   * and su_nationkey = n_nationkey and n_name = 'Germany'
   * group by su_name
   * order by numwait desc, su_name
   */

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // prepare nation selection
    val nSchema = ChTSchema.nationSch
    val nationQuery = new ScanQuery
    val nNameIndex = nSchema.getField("n_name").index

    val nationSelection = new CNFClause
    nationSelection.addPredicate(
      ScanQuery.CmpType.EQUAL, nNameIndex, new StringType("Germany"))
//    nationQuery.addSelection(nationSelection)

    val oo = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch)
    val orderline1 = oo.toDF()
    val orderline2 = oo.toDF()
    val supplier = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch).toDF()
    val stock = stockRdd(scc, new ScanQuery, ChTSchema.stockSch).toDF()
    val fnation = nationRdd(scc, nationQuery, nSchema).toDF()
      .filter($"n_name" === "Germany")
    val order = orderRdd(scc, new ScanQuery, ChTSchema.orderSch).toDF()

    val s_n = supplier.join(fnation, $"su_nationkey" === "n_nationkey")
    val s_s_n = s_n.join(stock, (($"s_w_id" * $"s_i_id")%10000) === $"su_suppkey")

    val res = orderline1.join(orderline2, ((orderline1("ol_o_id") !== orderline2("ol_o_id")) &&
      (orderline1("ol_w_id") !== orderline2("ol_w_id")) &&
      (orderline1("ol_d_id") !== orderline2("ol_d_id")) &&
      (orderline1("ol_delivery_d") > orderline2("ol_delivery_d"))))
      //ol_w_id = s_w_id and ol_i_id = s_i_id
    .join(s_s_n,
        orderline1("ol_w_id") === $"s_w_id" &&
        orderline1("ol_i_id") === $"s_i_id")
    .join(order,
        orderline1("ol_o_id") === $"o_id" &&
        orderline1("ol_w_id") === $"o_w_id" &&
        orderline1("ol_d_id") === $"o_d_id" &&
        orderline1("ol_delivery_d") > $"o_entry_d")
    .select($"su_name", $"o_id")
    .groupBy($"su_name")
      // todo is this the same? count(*) as numwait
    .agg(count($"o_id").as("numwait"))
    .orderBy($"numwait".desc, $"su_name")

    timeCollect(res, 21)
  }
}
