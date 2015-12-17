package ch.ethz.queries.chb

import ch.ethz.queries.ChQuery
import ch.ethz.tell.PredicateType.StringType
import ch.ethz.tell.{CNFClause, ScanQuery, TSparkContext}

/**
 * select	 i_name,
	 substr(i_data, 1, 3) as brand,
	 i_price,
	 count(distinct (mod((s_w_id * s_i_id),10000))) as supplier_cnt
from	 stock, item
where	 i_id = s_i_id
	 and i_data not like 'zz%'
	 and (mod((s_w_id * s_i_id),10000) not in
		(select su_suppkey
		 from supplier
		 where su_comment like '%bad%'))
group by i_name, substr(i_data, 1, 3), i_price
order by supplier_cnt desc
 */
//class Q16  extends ChQuery {
//
//  /**
//   * implemented in children classes and hold the actual query
//   */
//  override def execute(st: String, cm: String, cn: Int, cs: Long, mUrl: String): Unit = {
//    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
//
//    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
//    import org.apache.spark.sql.functions._
//    import sqlContext.implicits._
//
//    // prepare date selection
//    val iSchema = ChTSchema.itemSch
//    val itemQuery = new ScanQuery
//    val iDataIndex = iSchema.getField("i_data").index
//
//    val dataSelection = new CNFClause
//    dataSelection.addPredicate(
//      ScanQuery.CmpType.NOT_LIKE, iDataIndex, new StringType("zz%"))
////    itemQuery.addSelection(dataSelection)
//
//    val fsupplier = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch).toDF()
//      .filter($"su_comment".like("%bad%"))
//      .select($"su_suppkey")
//
//    val fitem = itemRdd(scc, itemQuery, iSchema).toDF()
//    .filter(!$"i_data".like("zz%"))
//
//    val stock = stockRdd(scc, new ScanQuery, ChTSchema.stockSch).toDF()
//    val res = stock.join(fsupplier, ( ($"s_w_id" * $"s_i_id")%10000 !== (fsupplier("su_suppkey")) ))
//    .join(fitem, $"i_id" === $"s_i_id")
//    .select($"i_name", $"i_data".substr(1, 3).as("brand"), $"i_price", $"s_w_id", $"s_i_id")
//    .groupBy($"i_name",$"brand", $"i_price")
//    .agg(countDistinct(($"s_w_id" * $"s_i_id")%10000).as("supplier_cnt"))
//    .orderBy("supplier_cnt")
//
//    timeCollect(res, 16)
//    scc.sparkContext.stop()
//  }
//
//}