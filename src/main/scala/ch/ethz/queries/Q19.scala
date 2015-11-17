package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * select	sum(ol_amount) as revenue
from	orderline, item
where	(
	  ol_i_id = i_id
          and i_data like '%a'
          and ol_quantity >= 1
          and ol_quantity <= 10
          and i_price between 1 and 400000
          and ol_w_id in (1,2,3)
	) or (
	  ol_i_id = i_id
	  and i_data like '%b'
	  and ol_quantity >= 1
	  and ol_quantity <= 10
	  and i_price between 1 and 400000
	  and ol_w_id in (1,2,4)
	) or (
	  ol_i_id = i_id
	  and i_data like '%c'
	  and ol_quantity >= 1
	  and ol_quantity <= 10
	  and i_price between 1 and 400000
	  and ol_w_id in (1,5,3)
	)
 */
class Q19 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

//    val aa = udf { (x: String) => x.matches("a") }
//    val bb = udf { (x: String) => x.matches("b") }
//    val cc = udf { (x: String) => x.matches("c") }

    val ol = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch)
    val it = itemRdd(scc, new ScanQuery, ChTSchema.itemSch)
    val forderline = ol.toDF().filter($"ol_quantity" >= 1 && $"ol_quantity" <= 10)
    val fitem = it.toDF().filter($"i_price" >= 1 && $"i_price" <= 40000)
    val res = forderline.join(fitem, $"ol_i_id" === $"i_id")
    .filter(
        (($"i_data".like("a")) && $"ol_w_id".isin(1, 2, 3)) ||
        (($"i_data".like("b")) && $"ol_w_id".isin(1, 2, 4)) ||
        (($"i_data".like("c")) && $"ol_w_id".isin(1, 5, 3)))
    .select($"ol_amount")
    .agg(sum($"ol_amount").as("revenue"))

    timeCollect(res, 19)
  }

}
