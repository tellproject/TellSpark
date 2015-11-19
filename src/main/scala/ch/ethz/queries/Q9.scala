package ch.ethz.queries

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset, LocalDateTime}
import java.util.{GregorianCalendar, Date}

import ch.ethz.tell.{ScanQuery, TSparkContext}
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

  override def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val supplier = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch).toDF()
    val fitem = itemRdd(scc, new ScanQuery, ChTSchema.itemSch).toDF().filter($"i_data".like("%BB"))
    val stock = stockRdd(scc, new ScanQuery, ChTSchema.stockSch).toDF()
    val orderline = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch).toDF()
    val orders = orderRdd(scc, new ScanQuery, ChTSchema.orderSch).toDF()
    val nation = nationRdd(scc, new ScanQuery, ChTSchema.nationSch).toDF()
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