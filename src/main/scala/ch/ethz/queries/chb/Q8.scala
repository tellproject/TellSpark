package ch.ethz.queries.chb

import java.time.Instant

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell.PredicateType.{IntType, StringType}
import ch.ethz.tell.{BufferType, CNFClause, ScanQuery, TSparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
 * Ch Query8
 */
class Q8 extends ChQuery {


  /**
   * select	 extract(year from o_entry_d) as l_year,
   * sum(case when n2.n_name = 'Germany' then ol_amount else 0 end) / sum(ol_amount) as mkt_share
   * from	 item, supplier, stock, orderline, orders, customer, nation n1, nation n2, region
   * where	 i_id = s_i_id and i_id = ol_i_id and ol_i_id = s_i_id and
   * ol_supply_w_id = s_w_id
   * and mod((s_w_id * s_i_id),10000) = su_suppkey
   * and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
   * and c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
   * and n1.n_nationkey = ascii(substr(c_state,1,1))
   * and n1.n_regionkey = r_regionkey
   * and ol_i_id < 1000
   * and r_name = 'Europe'
   * and su_nationkey = n2.n_nationkey
   * and o_entry_d between '2007-01-02 00:00:00.000000' and '2012-01-02 00:00:00.000000'
   * and i_data like '%b'
   * group by extract(year from o_entry_d)
   * order by l_year
   */
  val getYear = udf { (x: Long) => Instant.ofEpochSecond(x).toString.substring(0,4) }
  val mkr_share = udf { (x: String, y: Double) => if (x.equals("Germany")) y else 0 }

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import BufferType._
    // prepare date selection
    val oSchema = ChTSchema.orderSch
    val ordQry = new TScanQuery("order", tSparkContext.partNum.value, Big)
    val oEntryIndex = oSchema.getField("o_entry_d").index
//
    val dateSelectionLower = new CNFClause
    dateSelectionLower.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oEntryIndex, referenceDate2007)
//    orderQuery.addSelection(dateSelectionLower)
//
    val dateSelectionUpper = new CNFClause
    dateSelectionUpper.addPredicate(
      ScanQuery.CmpType.LESS_EQUAL, oEntryIndex, referenceDate2012)
//    //orderQuery.addSelection(dateSelectionUpper)
//
    // prepare orderline id selection
    val olSchema = ChTSchema.orderLineSch
    val olQry = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    val olIdIndex = olSchema.getField("ol_i_id").index

    val olIdSelection = new CNFClause
    olIdSelection.addPredicate(
      ScanQuery.CmpType.LESS, olIdIndex, new IntType(1000))
    olQry.addSelection(olIdSelection)

    // prepare region selection (not sure whether that helps)
    val rSchema = ChTSchema.regionSch
    val regionSelection = new CNFClause
    regionSelection.addPredicate(
      ScanQuery.CmpType.EQUAL, rSchema.getField("r_name").index, new StringType("Europe"))
    val regionQuery = new TScanQuery("region", tSparkContext.partNum.value, Small)
    val natQry = new TScanQuery("nation", tSparkContext.partNum.value, Small)
//    regionQuery.addSelection(regionSelection)

    val supQry = new TScanQuery("supplier", tSparkContext.partNum.value, Big)
    val cusQry = new TScanQuery("customer", tSparkContext.partNum.value, Big)
    val stkQry = new TScanQuery("stock", tSparkContext.partNum.value, Big)
    val itmQry = new TScanQuery("item", tSparkContext.partNum.value, Big)

    // supplier, stock, orderline, orders, customer, nation n1, nation n2
    val forderline = orderLineRdd(tSparkContext, olQry, ChTSchema.orderLineSch).toDF()
//      .filter($"ol_i_id" < 1000)
    val supplier = supplierRdd(tSparkContext, supQry, ChTSchema.supplierSch).toDF()
    val n = nationRdd(tSparkContext, natQry, ChTSchema.nationSch)
    val n1 = n.toDF()
    val n2 = n.toDF()
    val customer = customerRdd(tSparkContext, cusQry, ChTSchema.customerSch).toDF()
    val forder = orderRdd(tSparkContext, ordQry, oSchema).toDF()
//    .filter($"o_entry_d".between(20070102, 20120102))

    val stock = stockRdd(tSparkContext, stkQry, ChTSchema.stockSch).toDF()
    val fregion = regionRdd(tSparkContext, regionQuery, rSchema).toDF()
      .filter($"r_name" === "Europe")

    val fitem = itemRdd(tSparkContext, itmQry, ChTSchema.itemSch).toDF().filter($"i_data".like("%b"))
    val s_n2 = supplier.join(n2, $"su_nationkey" === n2("n_nationkey"))
    val r_n1 = fregion.join(n1, $"r_regionkey" === n1("n_regionkey"))

    //mod((s_w_id * s_i_id),10000) = su_suppkey
    val part_res1 = stock.join(s_n2, ($"s_w_id"*$"s_i_id")%10000 === s_n2("su_suppkey"))
      //and ol_i_id = s_i_id and ol_supply_w_id = s_w_id
    .join(forderline, ($"s_i_id" === forderline("ol_i_id") && ($"s_w_id" === forderline("ol_supply_w_id"))) )
    .join(fitem, (($"i_id" === $"s_i_id") && ($"i_id" === $"ol_i_id")))

    // n1.n_nationkey = ascii(substr(c_state,1,1)) and
    // c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
    val part_res2 = customer.join(r_n1, $"c_state".substr(1,1) === r_n1("n_nationkey"))
    .join(forder, (($"o_c_id" === $"c_id") && ($"c_w_id" === $"o_w_id") && ($"c_d_id" === $"o_d_id")))
    //ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
    val res = part_res1.join(part_res2,
      ( ($"ol_w_id" === $"o_w_id") &&
      ($"ol_d_id" === $"o_d_id") &&
      ($"ol_o_id" === $"o_id")))
      // todo check the "first function"
      .select( getYear($"o_entry_d").as("l_year"), $"ol_amount", part_res2("n_name"))
    .groupBy($"l_year")
    .agg(sum(mkr_share(part_res2("n_name"),$"ol_amount"))/sum($"ol_amount"))

    timeCollect(res, 8)
  }
}
