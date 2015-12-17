package ch.ethz.queries.chb

import java.time.Instant

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell.{BufferType, ScanQuery, TSparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
 * select	 n_name, extract(year from o_entry_d) as l_year, sum(ol_amount) as sum_profit
from	 item, stock, supplier, orderline, orders, nation
where	 ol_i_id = s_i_id
	 and ol_supply_w_id = s_w_id
	 and mod((s_w_id * s_i_id), 10000) = su_suppkey
	 and ol_w_id = o_w_id
	 and ol_d_id = o_d_id
	 and ol_o_id = o_id
	 and ol_i_id = i_id
	 and su_nationkey = n_nationkey
	 and i_data like '%BB'
group by n_name, extract(year from o_entry_d)
order by n_name, l_year desc
 */
class Q9 extends ChQuery {

  val getYear = udf { (x: Long) => Instant.ofEpochSecond(x).toString.substring(0,4) }

  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import BufferType._

    val supQry = new TScanQuery("supplier", tSparkContext.partNum.value, Small)
    val itmQry = new TScanQuery("item", tSparkContext.partNum.value, Big)
    val stkQry = new TScanQuery("stock", tSparkContext.partNum.value, Big)
    val olQry = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    val ordQry = new TScanQuery("order", tSparkContext.partNum.value, Big)
    val natQry = new TScanQuery("nation", tSparkContext.partNum.value, Small)

    val supplier = supplierRdd(tSparkContext, supQry, ChTSchema.supplierSch).toDF()
    val fitem = itemRdd(tSparkContext, itmQry, ChTSchema.itemSch).toDF().filter($"i_data".like("%BB"))
    val stock = stockRdd(tSparkContext, stkQry, ChTSchema.stockSch).toDF()
    val orderline = orderLineRdd(tSparkContext, olQry, ChTSchema.orderLineSch).toDF()
    val orders = orderRdd(tSparkContext, ordQry, ChTSchema.orderSch).toDF()
    val nation = nationRdd(tSparkContext, natQry, ChTSchema.nationSch).toDF()
    val s_n = supplier.join(nation, nation("n_nationkey") === $"su_nationkey")
    val part_res = stock.join(s_n, $"s_w_id"*$"s_i_id"%10000 === s_n("su_suppkey"))
      //ol_i_id = s_i_id and ol_supply_w_id = s_w_id
    .join(orderline, $"ol_i_id" === $"s_i_id" && $"ol_supply_w_id" === $"s_w_id")
    .join(fitem, $"ol_i_id" === fitem("i_id"))
    .join(orders, $"ol_w_id" === orders("o_w_id") && $"ol_d_id" === orders("o_d_id") && $"ol_o_id" === orders("o_id"))

    val res = part_res
      .select($"n_name", getYear($"o_entry_d").as("l_year"), $"ol_amount")
      //n_name, extract(year from o_entry_d) as l_year, sum(ol_amount) as sum_profit
      .groupBy($"n_name", $"l_year")
      .agg(sum($"ol_amount").as("sum_profit"))
      .orderBy($"n_name", $"l_year".desc)

    timeCollect(res, 9)
  }

}