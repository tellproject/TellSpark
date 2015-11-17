package ch.ethz.queries

import java.text.SimpleDateFormat
import java.util.{Date, GregorianCalendar}

import ch.ethz.tell.{ScanQuery, TSparkContext}
import org.apache.spark.sql.functions.udf

/**
 * select	 su_nationkey as supp_nation,
	 substr(c_state,1,1) as cust_nation,
	 extract(year from o_entry_d) as l_year,
	 sum(ol_amount) as revenue
from	 supplier, stock, orderline, orders, customer, nation n1, nation n2
where	 ol_supply_w_id = s_w_id
	 and ol_i_id = s_i_id
	 and mod((s_w_id * s_i_id), 10000) = su_suppkey
	 and ol_w_id = o_w_id
	 and ol_d_id = o_d_id
	 and ol_o_id = o_id
	 and c_id = o_c_id
	 and c_w_id = o_w_id
	 and c_d_id = o_d_id
	 and su_nationkey = n1.n_nationkey
	 and ascii(substr(c_state,1,1)) = n2.n_nationkey
	 and (
		(n1.n_name = 'Germany' and n2.n_name = 'Cambodia')
	     or
		(n1.n_name = 'Cambodia' and n2.n_name = 'Germany')
	     )
	 and ol_delivery_d between '2007-01-02 00:00:00.000000' and '2012-01-02 00:00:00.000000'
group by su_nationkey, substr(c_state,1,1), extract(year from o_entry_d)
order by su_nationkey, cust_nation, l_year
 */
class Q7 extends ChQuery {

  //TODO double check
  val sdf = new SimpleDateFormat("yyyy")
  val cal = new GregorianCalendar()

  val getYear = udf { (x: Long) => timestampToString(x) }

  def timestampToString(input : Long): String = {
    val dt = new Date(input)
    sdf.setCalendar(cal)
    cal.setTime(dt)
    sdf.format(dt).substring(0, 4)
  }

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String) = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    // supplier, stock, orderline, orders, customer, nation n1, nation n2
    val supp = supplierRdd(scc, new ScanQuery())
    val stk = stockRdd(scc, new ScanQuery())
    val ol = orderLineRdd(scc, new ScanQuery())
    val oo = ordersRdd(scc, new ScanQuery())
    val cc = customerRdd(scc, new ScanQuery())
    val nn1 = nationRdd(scc, new ScanQuery())
    val nn2 = nationRdd(scc, new ScanQuery())

    val forderline = ol.toDF().filter($"ol_delivery_d" >= 20070102 && $"ol_delivery_d" <= 20120102 )
    val supplier = supp.toDF()
    val n1 = nn1.toDF()
    val n2 = nn2.toDF()
    val customer = cc.toDF()
    val order = oo.toDF()
    val orderline = ol.toDF()
    val stock = stk.toDF()

    val suppNation = supplier.join(n1, $"su_nationkey" === n1("n_nationkey"))
    .join(n2,
    (((n1("n_name") === "Germany") && (n2("n_name") === "Cambodia")) ||
      ((n1("n_name") === "Cambodia") && (n2("n_name") === "Germany"))) )

    val part_res = customer.join(suppNation, n2("n_nationkey") === customer("c_state").substr(1,1))
    .join(order, (order("o_c_id") === customer("c_id")) &&
      (order("o_w_id") === customer("c_w_id")) &&
      (order("o_d_id") === customer("c_d_id")))
    .join(forderline, ((forderline("ol_w_id") === order("o_w_id")) &&
      (forderline("ol_d_id") === order("o_d_id")) &&
      (forderline("ol_o_id") === order("o_id"))))
    .join(stock, ((stock("s_w_id") === forderline("ol_supply_w_id")) &&
      (stock("s_i_id") === forderline("ol_i_id")) &&
      ( stock("s_i_id")*stock("s_w_id") % 10000 === suppNation("su_suppkey"))))
    val res = part_res
      .select($"su_nationkey".as("supp_nation"),
        $"c_state".substr(1,1).as("cust_nation"),
        getYear($"o_entry_d").as("l_year")
      )
      .groupBy(supplier("su_nationkey"), customer("c_state").substr(1,1), getYear(order("o_entry_d")))
      .agg(sum("ol_amount").as("revenue"))
      .sort($"supp_nation", $"cust_nation", $"l_year")

    timeCollect(res, 7)
  }
}

