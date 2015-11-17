package ch.ethz.queries

import ch.ethz.tell._
import ch.ethz.TellClientFactory
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Query2
 */
class Q2 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // convert an RDDs to a DataFrames
    val stk = stockRdd(scc, new ScanQuery)
    var cnt = stk.count
    println("=================== Q2 ===================stock:" + cnt )
    val stock = stk.toDF()

    val spp = supplierRdd(scc, new ScanQuery)
    cnt = spp.count
    println("=================== Q2 ===================supplier:" + cnt )
    val supplier = spp.toDF()

    val nn = new TRDD[TRecord](scc, "nation", new ScanQuery(), ChTSchema.nationSch).map(r => {
      Nation(r.getValue("N_NATIONKEY").asInstanceOf[Short],
        r.getValue("N_NAME").asInstanceOf[String],
        r.getValue("N_REGIONKEY").asInstanceOf[Short],
        r.getValue("N_COMMENT").asInstanceOf[String])
    })
    cnt = nn.count
    println("=================== Q2 ===================nation:" + cnt) 
   val nation = nn.toDF()

    val rrr = new TRDD[TRecord](scc, "region", new ScanQuery(), ChTSchema.regionSch).map(r => {
      Region(r.getValue("R_REGIONKEY").asInstanceOf[Short],
        r.getValue("R_NAME").asInstanceOf[String],
        r.getValue("R_COMMENT").asInstanceOf[String])
    })
    cnt = rrr.count
    println("=================== Q2 ===================region:" + cnt )
    val region = rrr.toDF()

    /**
     * Inner query
     * (select s_i_id as m_i_id, min(s_quantity) as m_s_quantity from
     * stock, supplier, nation, region
     *     where mod((s_w_id*s_i_id),10000)=su_suppkey and su_nationkey=n_nationkey
     *     and n_regionkey=r_regionkey and r_name like 'Europ%' group by s_i_id) m
     */
    val minEuQty = stock.join(supplier, (stock("S_W_ID")*stock("S_I_ID")%10000) === supplier("SU_SUPPKEY"))
    .join(nation, $"SU_NATIONKEY" === nation("N_NATIONKEY"))
    .join(region, $"N_REGIONKEY" === region("R_REGIONKEY"))
    .filter(region("R_NAME").startsWith("Europ"))
    .groupBy($"S_I_ID")
    .agg(min($"S_QUANTITY").as("M_S_QUANTITY")).select("S_I_ID as M_I_ID", "M_S_QUANTITY")

    val item = new TRDD[TRecord](scc, "item", new ScanQuery(), ChTSchema.itemSch).map(r => {
      Item(r.getValue("I_ID").asInstanceOf[Int],
        r.getValue("I_IM_ID").asInstanceOf[Short],
        r.getValue("I_NAME").asInstanceOf[String],
        r.getValue("I_PRICE").asInstanceOf[Double],
        r.getValue("I_DATA").asInstanceOf[String])
    }).toDF()

    /**
     * select su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
     * from item, supplier, stock, nation, region, m
     * where i_id = s_i_id and mod((s_w_id * s_i_id), 10000) = su_suppkey and su_nationkey = n_nationkey
     * and n_regionkey = r_regionkey and i_data like '%b' and r_name like 'Europ%'
     * and i_id=m_i_id and s_quantity = m_s_quantity
     * order by n_name, su_name, i_id
     */
    //ToDo push downs
    val res = stock
      .join(item, $"S_I_ID" === item("I_ID"))
      .join(supplier, (stock("S_W_ID")*stock("S_I_ID")%10000) === supplier("SU_SUPPKEY"))
      .join(nation, $"SU_NATIONKEY" === nation("N_NATIONKEY"))
      .join(region, $"N_REGIONKEY" === region("R_REGIONKEY"))
      .filter(item("I_DATA").endsWith("b"))
      .filter(region("R_NAME").startsWith("Europ"))
      .join(minEuQty, (($"I_ID" === minEuQty("M_I_ID")) && ($"S_QUANTITY" === minEuQty("M_S_QUANTITY"))))
      .orderBy(nation("N_NAME"), supplier("SU_NAME"), item("I_ID"))
    //outputDF(res)
  }
}
