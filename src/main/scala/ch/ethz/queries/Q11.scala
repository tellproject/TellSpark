package ch.ethz.queries

import ch.ethz.tell.PredicateType.StringType
import ch.ethz.tell.{CNFClause, ScanQuery, TSparkContext}

/**
 * Query11
 * Created by renatomarroquin on 2015-11-18.
 */
class Q11 extends ChQuery {

  /**
   * select	 s_i_id, sum(s_order_cnt) as ordercount
from	 stock, supplier, nation
where	 mod((s_w_id * s_i_id),10000) = su_suppkey
	 and su_nationkey = n_nationkey
	 and n_name = 'Germany'
group by s_i_id
having   sum(s_order_cnt) >
		(
  select sum(s_order_cnt) * .005
		from stock, supplier, nation
		where mod((s_w_id * s_i_id),10000) = su_suppkey
		and su_nationkey = n_nationkey
		and n_name = 'Germany'
  )
order by ordercount desc
   */

  override def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // prepare nation selection
    val nSchema = ChTSchema.nationSch
    val nationQuery = new ScanQuery
    val nNameIndex = nSchema.getField("n_name").index

    val nationSelection = new CNFClause
    nationSelection.addPredicate(
      ScanQuery.CmpType.EQUAL, nNameIndex, new StringType("Germany"))
//    nationQuery.addSelection(nationSelection)

    val stock = stockRdd(scc, new ScanQuery, ChTSchema.stockSch).toDF()
    val supplier = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch).toDF()
    val fnation = nationRdd(scc, nationQuery, nSchema).toDF()
      .filter($"n_name" === "Germany")

    val inner_res = supplier.join(fnation, $"su_nationkey" === fnation("n_nationkey"))
    .join(stock, $"su_suppkey" === (stock("s_w_id")*stock("s_i_id")%10000))
    .select($"s_order_cnt")
    .agg((sum($"s_order_cnt")*0.005).as("sum_order"))

    val res = supplier.join(fnation, $"su_nationkey" === fnation("n_nationkey"))
      .join(stock, $"su_suppkey" === (stock("s_w_id")*stock("s_i_id")%10000))
    .select($"s_i_id", $"s_order_cnt")
    .groupBy($"s_i_id")
    .agg(sum($"s_order_cnt").as("ordercount"))
      //TODO to be checked
    .join(inner_res, $"ordercount" > inner_res("sum_order"))

    timeCollect(res, 11)
    scc.sparkContext.stop()
  }

}
