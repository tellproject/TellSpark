package ch.ethz.queries

import ch.ethz.TellClientFactory
import ch.ethz.tell.{TSparkContext, ScanQuery, TRecord, TRDD}

/**
 * Query5
 * select	 n_name,
	 sum(ol_amount) as revenue
from	 customer, orders, orderline, stock, supplier, nation, region
where	 c_id = o_c_id
	 and c_w_id = o_w_id
	 and c_d_id = o_d_id

	 and ol_o_id = o_id
	 and ol_w_id = o_w_id
	 and ol_d_id = o_d_id

	 and ol_w_id = s_w_id
	 and ol_i_id = s_i_id

	 and mod((s_w_id * s_i_id),10000) = su_suppkey
	 and ascii(substr(c_state,1,1)) = su_nationkey

	 and su_nationkey = n_nationkey

	 and n_regionkey = r_regionkey
	 and r_name = 'Europe'

	 and o_entry_d >= '2007-01-02 00:00:00.000000'
group by n_name
order by revenue desc
 */
class Q5 extends ChQuery {
  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    println("[TELL] PARAMETERS USED: " + TellClientFactory.toString())

    //orderline
    val oo = new TRDD[TRecord](scc, "order-line", new ScanQuery, ChTSchema.orderLineSch).map(r => {
      OrderLine(r.getValue("OL_O_ID").asInstanceOf[Int],
        r.getValue("OL_D_ID").asInstanceOf[Short],
        r.getValue("OL_W_ID").asInstanceOf[Int],
        r.getValue("OL_NUMBER").asInstanceOf[Short],
        r.getValue("OL_I_ID").asInstanceOf[Int],
        r.getValue("OL_SUPPLY_W_ID").asInstanceOf[Int],
        r.getValue("OL_DELIVERY_D").asInstanceOf[Long],
        r.getValue("OL_QUANTITY").asInstanceOf[Short],
        r.getValue("OL_AMOUNT").asInstanceOf[Long],
        r.getValue("OL_DIST_INFO").asInstanceOf[String])
    })
    val orderline = oo.toDF()
    //customer
    val cc = new TRDD[TRecord](scc, "customer", new ScanQuery(), ChTSchema.customerSch).map(r => {
      Customer(r.getValue("C_ID").asInstanceOf[Int],
        r.getValue("C_D_ID").asInstanceOf[Int],
        r.getValue("C_W_ID").asInstanceOf[Int],
        r.getValue("C_FIRST").asInstanceOf[String],
        r.getValue("C_MIDDLE").asInstanceOf[String],
        r.getValue("C_LAST").asInstanceOf[String],
        r.getValue("C_STREET_1").asInstanceOf[String],
        r.getValue("C_STREET_2").asInstanceOf[String],
        r.getValue("C_CITY").asInstanceOf[String],
        r.getValue("C_STATE").asInstanceOf[String],
        r.getValue("C_ZIP").asInstanceOf[String],
        r.getValue("C_PHONE").asInstanceOf[String],
        r.getValue("C_SINCE").asInstanceOf[Long],
        r.getValue("C_CREDIT").asInstanceOf[String],
        r.getValue("C_CREDIT_LIM").asInstanceOf[Double],
        r.getValue("C_DISCOUNT").asInstanceOf[Double],
        r.getValue("C_BALANCE").asInstanceOf[Double],
        r.getValue("C_YTD_PAYMENT").asInstanceOf[Double],
        r.getValue("C_PAYMENT_CNT").asInstanceOf[Short],
        r.getValue("C_DELIVERY_CNT").asInstanceOf[Short],
        r.getValue("C_DATA").asInstanceOf[String],
        r.getValue("C_N_NATIONKEY").asInstanceOf[Int]
      )
    })
    val customer = cc.toDF()
    // orders
    val orr = new TRDD[TRecord](scc, "orders", new ScanQuery(), ChTSchema.orderSch).map(r => {
      Order(r.getValue("O_ID").asInstanceOf[Int],
        r.getValue("O_D_ID").asInstanceOf[Short],
        r.getValue("O_W_ID").asInstanceOf[Int],
        r.getValue("O_C_ID").asInstanceOf[Short],
        r.getValue("O_ENTRY_D").asInstanceOf[Long],
        r.getValue("O_CARRIER_ID").asInstanceOf[Short],
        r.getValue("O_OL_CNT").asInstanceOf[Short],
        r.getValue("O_ALL_LOCAL").asInstanceOf[Short]
      )
    })
    val orders = orr.toDF()
    // stock
    val stt = new TRDD[TRecord](scc, "stock", new ScanQuery(), ChTSchema.stockSch).map(r => {
      new Stock(r.getValue("S_I_ID").asInstanceOf[Int],
        r.getValue("S_W_ID").asInstanceOf[Short],
        r.getValue("S_QUANTITY").asInstanceOf[Int],
        r.getValue("S_DIST_01").asInstanceOf[String],
        r.getValue("S_DIST_02").asInstanceOf[String],
        r.getValue("S_DIST_03").asInstanceOf[String],
        r.getValue("S_DIST_04").asInstanceOf[String],
        r.getValue("S_DIST_05").asInstanceOf[String],
        r.getValue("S_DIST_06").asInstanceOf[String],
        r.getValue("S_DIST_07").asInstanceOf[String],
        r.getValue("S_DIST_08").asInstanceOf[String],
        r.getValue("S_DIST_09").asInstanceOf[String],
        r.getValue("S_DIST_10").asInstanceOf[String],
        r.getValue("S_YTD").asInstanceOf[Int],
        r.getValue("S_ORDER_CNT").asInstanceOf[Short],
        r.getValue("S_REMOTE_CNT").asInstanceOf[Short],
        r.getValue("S_DATA").asInstanceOf[String]
        , r.getValue("S_SU_SUPPKEY").asInstanceOf[Int]
      )
    }).toDF()
    val stock = stt.toDF()
    //supplier
    val spp = new TRDD[TRecord](scc, "supplier", new ScanQuery(), ChTSchema.supplierSch).map(r => {
      Supplier(r.getValue("SU_SUPPKEY").asInstanceOf[Short],
        r.getValue("SU_NAME").asInstanceOf[String],
        r.getValue("SU_ADDRESS").asInstanceOf[String],
        r.getValue("SU_NATIONKEY").asInstanceOf[Short],
        r.getValue("SU_PHONE").asInstanceOf[String],
        r.getValue("SU_ACCTBAL").asInstanceOf[Double],
        r.getValue("SU_COMMENT").asInstanceOf[String])
    })
    val supplier = spp.toDF()
    //nation
    val nn = new TRDD[TRecord](scc, "nation", new ScanQuery(), ChTSchema.nationSch).map(r => {
      Nation(r.getValue("N_NATIONKEY").asInstanceOf[Short],
        r.getValue("N_NAME").asInstanceOf[String],
        r.getValue("N_REGIONKEY").asInstanceOf[Short],
        r.getValue("N_COMMENT").asInstanceOf[String])
    })
    val nation = nn.toDF()
    //region
    val rrr = new TRDD[TRecord](scc, "region", new ScanQuery(), ChTSchema.regionSch).map(r => {
      Region(r.getValue("R_REGIONKEY").asInstanceOf[Short],
        r.getValue("R_NAME").asInstanceOf[String],
        r.getValue("R_COMMENT").asInstanceOf[String])
    })
    val region = rrr.toDF()

    val forder = orders.filter(orders("o_entry_d").geq(20070102))
    val fregion = region.filter(region("r_name").eqNullSafe("Europe"))
    val part_res = customer.join(forder, ($"c_id" === forder("O_C_ID")) &&
      ($"c_w_id" === forder("o_w_id")) &&
      ($"c_d_id" === forder("o_d_id")))
    .join(orderline, (orderline("ol_o_id") === forder("o_id")) &&
      (orderline("ol_w_id") === forder("o_w_id")) &&
      (orderline("ol_d_id") === forder("o_d_id")))
    .join(stock, (orderline("ol_w_id") === stock("s_w_id")) &&
      (orderline("ol_i_id") === stock("s_i_id")) )

    val jsupp = supplier.join(nation, $"su_nationkey" === nation("n_nationkey"))
    .join(fregion, nation("n_regionkey") === region("r_regionkey"))

    val part_2 = part_res
      .join(jsupp, (part_res("s_w_id")*part_res("s_i_id")%10000 === jsupp("su_suppkey")) &&
      (part_res("c_state").substr(1,1).eq(jsupp("su_nationkey"))))
    //todo push down filter
    val res = part_2.groupBy(part_res("n_name"))
    .agg(part_res("ol_amount").as("revenue"))
    .orderBy("revenue")
    .select("n_name", "revenue")

    timeCollect(res, 5)

  }

}
