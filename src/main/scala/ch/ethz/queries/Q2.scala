package ch.ethz.queries

import ch.ethz.tell.{TellSchema, TellRecord, ScanQuery, TellRDD}
import ch.ethz.TellClientFactory
import org.apache.spark.{SparkContext, SparkConf}

/**
  select su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
  from item, supplier, stock, nation, region,
      (select
        s_i_id as m_i_id,
        min(s_quantity) as m_s_quantity
        from
      stock, supplier, nation, region
      where mod((s_w_id*s_i_id),10000)=su_suppkey
      and su_nationkey=n_nationkey
      and n_regionkey=r_regionkey
      and r_name like 'Europ%'
      group by s_i_id) m
  where 	 i_id = s_i_id
  and mod((s_w_id * s_i_id), 10000) = su_suppkey
  and su_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and i_data like '%b'
  and r_name like 'Europ%'
  and i_id=m_i_id
  and s_quantity = m_s_quantity
  order by n_name, su_name, i_id
 */
object Q2 {

  //TODO move to sparkContext
  val conf = new SparkConf()
  val sc = new SparkContext(conf)

  def main(args : Array[String]) {
    var st = "192.168.0.11:7241"
    var cm = "192.168.0.11:7242"
    var cn = 4
    var cs = 5120000

    // client properties
    if (args.length == 4) {
      st = args(0)
      cm = args(1)
      cn = args(2).toInt
      cs = args(3).toInt
    }

    TellClientFactory.storageMng = st
    TellClientFactory.commitMng = cm
    TellClientFactory.chNumber = cn
    TellClientFactory.chSize = cs

    /**
     * (select
        s_i_id as m_i_id,
        min(s_quantity) as m_s_quantity
        from
      stock, supplier, nation, region
      where mod((s_w_id*s_i_id),10000)=su_suppkey
      and su_nationkey=n_nationkey
      and n_regionkey=r_regionkey
      and r_name like 'Europ%'
      group by s_i_id) m
     */
    // schema to be read
    val sch: TellSchema = CHSchema.orderLineSch

    // rdd creation
    val stockRdd = new TellRDD[TellRecord](sc, "stock", new ScanQuery(), CHSchema.stockSch)
    val supplierRdd = new TellRDD[TellRecord](sc, "supplier", new ScanQuery(), CHSchema.supplierSch)
    val nationRdd = new TellRDD[TellRecord](sc, "nation", new ScanQuery(), CHSchema.nationSch)
    val regionRdd = new TellRDD[TellRecord](sc, "region", new ScanQuery(), CHSchema.regionSch)

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
    val m = suppMapped.join(stockMapped).join(natMapped).join(regMapped).groupBy(_._1)
    println("=============COLLECTING==============")
//    tellRdd.collect()
    //    println("[TUPLES] %d".format(result.length))

  }

}
