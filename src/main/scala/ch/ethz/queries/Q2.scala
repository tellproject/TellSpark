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
    val stock = new TRDD[TRecord](scc, "stock", new ScanQuery(), ChTSchema.stockSch).map(r => {
      Stock(r.getField("S_I_ID").asInstanceOf[Int],
        r.getField("S_W_ID").asInstanceOf[Short],
        r.getField("S_QUANTITY").asInstanceOf[Int],
        r.getField("S_DIST_01").asInstanceOf[String],
        r.getField("S_DIST_02").asInstanceOf[String],
        r.getField("S_DIST_03").asInstanceOf[String],
        r.getField("S_DIST_04").asInstanceOf[String],
        r.getField("S_DIST_05").asInstanceOf[String],
        r.getField("S_DIST_06").asInstanceOf[String],
        r.getField("S_DIST_07").asInstanceOf[String],
        r.getField("S_DIST_08").asInstanceOf[String],
        r.getField("S_DIST_09").asInstanceOf[String],
        r.getField("S_DIST_10").asInstanceOf[String],
        r.getField("S_YTD").asInstanceOf[Int],
        r.getField("S_ORDER_CNT").asInstanceOf[Short],
        r.getField("S_REMOTE_CNT").asInstanceOf[Short],
        r.getField("S_DATA").asInstanceOf[String]
        //, r.getField("S_SU_SUPPKEY").asInstanceOf[Int]
      )
    }).toDF()

    val supplier = new TRDD[TRecord](scc, "supplier", new ScanQuery(), ChTSchema.supplierSch).map(r => {
      Supplier(r.getField("SU_SUPPKEY").asInstanceOf[Short],
        r.getField("SU_NAME").asInstanceOf[String],
        r.getField("SU_ADDRESS").asInstanceOf[String],
        r.getField("SU_NATIONKEY").asInstanceOf[Short],
        r.getField("SU_PHONE").asInstanceOf[String],
        r.getField("SU_ACCTBAL").asInstanceOf[Double],
        r.getField("SU_COMMENT").asInstanceOf[String])
    }).toDF()

    val nation = new TRDD[TRecord](scc, "nation", new ScanQuery(), ChTSchema.nationSch).map(r => {
      Nation(r.getField("N_NATIONKEY").asInstanceOf[Short],
        r.getField("N_NAME").asInstanceOf[String],
        r.getField("N_REGIONKEY").asInstanceOf[Short],
        r.getField("N_COMMENT").asInstanceOf[String])
    }).toDF()

    val region = new TRDD[TRecord](scc, "region", new ScanQuery(), ChTSchema.regionSch).map(r => {
      Region(r.getField("R_REGIONKEY").asInstanceOf[Short],
        r.getField("R_NAME").asInstanceOf[String],
        r.getField("R_COMMENT").asInstanceOf[String])
    }).toDF()

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

    val item = new TRDD[TRecord](scc, "item", new ScanQuery(), ChTSchema.stockSch).map(r => {
      Item(r.getField("I_ID").asInstanceOf[Int],
        r.getField("I_IM_ID").asInstanceOf[Short],
        r.getField("I_NAME").asInstanceOf[String],
        r.getField("I_PRICE").asInstanceOf[Double],
        r.getField("I_DATA").asInstanceOf[String])
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
