package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * select	sum(ol_amount) / 2.0 as avg_yearly
from	orderline, (select   i_id, avg(ol_quantity) as a
		    from     item, orderline
		    where    i_data like '%b'
			     and ol_i_id = i_id
		    group by i_id) t
where	ol_i_id = t.i_id
	and ol_quantity < t.a
 */
class Q17 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val ol = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch)
    val orderline1 = ol.toDF()
    val orderline2 = ol.toDF()
    val it = itemRdd(scc, new ScanQuery, ChTSchema.itemSch)
    val fitem = it.toDF().filter($"i_data".like("%d"))
    val t = fitem.join(orderline1, fitem("i_id") === orderline1("ol_i_id"))
      .select(fitem("i_id"))
    .groupBy(fitem("i_id"))
    .agg(avg(orderline1("ol_quantity").as("a")))

    val res = orderline2
      .join(t, t("i_id") === $"ol_i_id" && $"ol_quantity" < t("a"))
      .agg(sum("ol_amount")./(2.0).as("avg_yearly"))

    timeCollect(res, 17)

  }

}
