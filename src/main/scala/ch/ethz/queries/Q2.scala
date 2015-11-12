package ch.ethz.queries

import ch.ethz.tell._
import ch.ethz.TellClientFactory
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Query2
 * select su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
 * from item, supplier, stock, nation, region,
 *     (select s_i_id as m_i_id, min(s_quantity) as m_s_quantity from stock, supplier, nation, region
 *     where mod((s_w_id*s_i_id),10000)=su_suppkey and su_nationkey=n_nationkey
 *     and n_regionkey=r_regionkey and r_name like 'Europ%' group by s_i_id) m
 * where i_id = s_i_id and mod((s_w_id * s_i_id), 10000) = su_suppkey and su_nationkey = n_nationkey
 * and n_regionkey = r_regionkey and i_data like '%b' and r_name like 'Europ%' and i_id=m_i_id
 * and s_quantity = m_s_quantity
 * order by n_name, su_name, i_id
 */
class Q2 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String, appName:String): Unit = {
    /**
     * (select s_i_id as m_i_id, min(s_quantity) as m_s_quantity
     * from stock, supplier, nation, region
     * where mod((s_w_id*s_i_id),10000) = su_suppkey and su_nationkey=n_nationkey
     * and n_regionkey=r_regionkey and r_name like 'Europ%' group by s_i_id) m
     */
    // rdd creation
    val scc = new TSparkContext(mUrl, appName, st, cm, cn, cs)
    println("[TELL] PARAMETERS USED: " + TellClientFactory.toString())

    val stockRdd = new TRDD[TRecord](scc, "stock", new ScanQuery(), stockSch)
    val supplierRdd = new TRDD[TRecord](scc, "supplier", new ScanQuery(), supplierSch)
    val nationRdd = new TRDD[TRecord](scc, "nation", new ScanQuery(), nationSch)
    val regionRdd = new TRDD[TRecord](scc, "region", new ScanQuery(), regionSch)

    val suppMapped = supplierRdd.map( r => (r.getField("SU_SUPPKEY").asInstanceOf[Int], r))
    val stockMapped = stockRdd.map( r => {
      val op = r.getField("S_W_ID").asInstanceOf[Short]*r.getField("S_I_ID").asInstanceOf[Short]%10000
      (op, r)
    })

    val natMapped = nationRdd.map( r => (r.getField("N_NATIONKEY").asInstanceOf[Int], r))
    val regMapped = regionRdd.filter( r => {
      r.getField("R_NAME").asInstanceOf[String].contains("Europ")
    }).map( r => (r.getField("R_REGIONKEY").asInstanceOf[Int], r))

    //TODO Map only the fields needed
    val suppJstock = suppMapped.join(stockMapped).map(r => (r._1, r._2._2))
    val natJreg = natMapped.join(regMapped).map(r => (r._1, r._2._1))
    val m = suppJstock.join(natJreg).groupBy(_._1).map(r => {
        var min = Int.MaxValue
        var mIid = -1
        for (x <- r._2) {
          if (min > x._2._1.getField("S_QUANTITY").asInstanceOf[Int])
            min = x._2._1.getField("S_QUANTITY").asInstanceOf[Int]
            mIid = x._2._1.getField("S_I_ID").asInstanceOf[Int]
        }
        (r._1, (mIid, min))
      })
    //TODO to be continued . . .
    // tellRdd.collect()
    // println("[TUPLES] %d".format(result.length))
  }
}
