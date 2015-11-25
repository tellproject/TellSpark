package ch.ethz.queries.chb

import ch.ethz.queries.ChQuery
import ch.ethz.tell._

/**
 * Ch Query2
 */
class Q2 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Long, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // convert an RDDs to a DataFrames
    val stk = stockRdd(scc, new ScanQuery, ChTSchema.stockSch)
    if (logger.isDebugEnabled) {
      logger.debug("[Query2] %s. Tuples:%d".format("stock", stk.count))
    }
    val stock = stk.toDF()

    val spp = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch)
    if (logger.isDebugEnabled) {
      logger.debug("[Query2] %s. Tuples:%d".format("supplier", spp.count))
    }
    val supplier = spp.toDF()

    val nn = nationRdd(scc, new ScanQuery, ChTSchema.nationSch)
    if (logger.isDebugEnabled) {
      logger.debug("[Query2] %s. Tuples:%d".format("nation", nn.count))
    }
   val nation = nn.toDF()

    val rrr = regionRdd(scc, new ScanQuery, ChTSchema.regionSch)
    if (logger.isDebugEnabled) {
      logger.debug("[Query2] %s. Tuples:%d".format("region", rrr.count))
    }
    val region = rrr.toDF()

    /**
     * Inner query
     * (select s_i_id as m_i_id, min(s_quantity) as m_s_quantity from
     * stock, supplier, nation, region
     *     where mod((s_w_id*s_i_id),10000)=su_suppkey and su_nationkey=n_nationkey
     *     and n_regionkey=r_regionkey and r_name like 'Europ%' group by s_i_id) m
     */
    val minEuQty = stock.join(supplier, (stock("s_w_id")*stock("s_i_id")%10000) === supplier("su_suppkey"))
    .join(nation, $"su_nationkey" === nation("n_nationkey"))
    .join(region, $"n_regionkey" === region("r_regionkey"))
    .filter(region("r_name").startsWith("Europ"))
    .groupBy($"s_i_id")
    .agg(min($"s_quantity").as("m_s_quantity")).select($"s_i_id".as("m_i_id"), $"m_s_quantity")

    val item = itemRdd(scc, new ScanQuery, ChTSchema.itemSch).toDF()

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
      .join(item, $"s_i_id" === item("i_id"))
      .join(supplier, (stock("s_w_id")*stock("s_i_id")%10000) === supplier("su_suppkey"))
      .join(nation, $"su_nationkey" === nation("n_nationkey"))
      .join(region, $"n_regionkey" === region("r_regionkey"))
      .filter(item("i_data").endsWith("b"))
      .filter(region("r_name").startsWith("Europ"))
      .join(minEuQty, (($"i_id" === minEuQty("m_i_id")) && ($"s_quantity" === minEuQty("m_s_quantity"))))
      .orderBy(nation("n_name"), supplier("su_name"), item("i_id"))

    timeCollect(res, 2)
    scc.sparkContext.stop()
  }
}
