package ch.ethz.queries.chb

import ch.ethz.queries.ChQuery
import ch.ethz.tell.PredicateType.StringType
import ch.ethz.tell.{CNFClause, ScanQuery, TSparkContext}

/**
  * Query 20
  */
class Q20 extends ChQuery {

  /**
   * select	 su_name, su_address
   * from supplier, nation
   * where su_suppkey in (
   *    select  mod(s_i_id * s_w_id, 10000)
   *    from stock, orderline
   *    where s_i_id in (
   *       select i_id
   *       from item
   *       where i_data like 'co%'
   *    )
   *    and ol_i_id=s_i_id and ol_delivery_d > '2010-05-23 12:00:00'
   *    group by s_i_id, s_w_id, s_quantity
   *    having 2*s_quantity > sum(ol_quantity)
   * )
   * and su_nationkey = n_nationkey and n_name = 'Germany'
   * order by su_name
   */

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Long, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // prepare date selection
    val olSchema = ChTSchema.orderLineSch
    val orderLineQuery = new ScanQuery
    val oDeliveryIndex = olSchema.getField("ol_delivery_d").index

    val dateSelection = new CNFClause
    dateSelection.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oDeliveryIndex, referenceDate2010)
    orderLineQuery.addSelection(dateSelection)

    // prepare nation selection
    val nSchema = ChTSchema.nationSch
    val nationQuery = new ScanQuery
    val nNameIndex = nSchema.getField("n_name").index

    val nationSelection = new CNFClause
    nationSelection.addPredicate(
      ScanQuery.CmpType.EQUAL, nNameIndex, new StringType("Germany"))
    nationQuery.addSelection(nationSelection)

    // prepare date selection
    val iSchema = ChTSchema.itemSch
    val itemQuery = new ScanQuery
    val iDataIndex = iSchema.getField("i_data").index

    val dataSelection = new CNFClause
    dataSelection.addPredicate(
      ScanQuery.CmpType.LIKE, iDataIndex, new StringType("co%"))
//    itemQuery.addSelection(dataSelection)

    //select i_id from item where i_data like 'co%'
    val fitem = itemRdd(scc, itemQuery, iSchema).toDF()
      .filter($"i_data".like("co%"))
      .select($"i_id")
    val forderline = orderLineRdd(scc, orderLineQuery, olSchema).toDF()
//      .filter($"ol_delivery_d" > 20100523)
    val stock = stockRdd(scc, new ScanQuery, ChTSchema.stockSch).toDF()
    val supplier = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch).toDF()
    val fnation = nationRdd(scc, nationQuery, nSchema).toDF()
      .filter($"n_name" === "Germany")

    val inner_query = stock
      .join(forderline, (forderline("ol_i_id")===$"s_i_id"))
      .join(fitem, $"s_i_id".isin(fitem("i_id")) )//TODO do with a join with item?
      .groupBy($"s_i_id", $"s_w_id", $"s_quantity")
      .agg(sum($"ol_quantity").as("sum_qty"))
      .filter($"s_quantity".*(2) > $"sum_qty")
      .select((($"s_i_id" * $"s_w_id")%10000).as("inner_suppkey"))

    //TODO verify the <isin> operation
    val res = supplier.join(fnation, fnation("n_nationkey") === $"su_nationkey")
      .join(inner_query, $"su_suppkey" === inner_query("inner_suppkey"))
//    .filter($"su_suppkey".isin(inner_query("inner_suppkey")))

    timeCollect(res, 20)
    scc.sparkContext.stop()
  }

}
