package ch.ethz.queries.tpch

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import ch.ethz.queries.main.scala.TpchQuery

/**
 * TPC-H Query 12
 * Savvas Savvides <ssavvides@us.ibm.com>
 *
 */
class Q12 extends TpchQuery {

  import sqlContext.implicits._

  override def execute(): Unit = {

    val mul = udf { (x: Double, y: Double) => x * y }
    val highPriority = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
    val lowPriority = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

    val res = lineitem.filter((
      $"l_shipmode" === "MAIL" || $"l_shipmode" === "SHIP") &&
      $"l_commitdate" < $"l_receiptdate" &&
      $"l_shipdate" < $"l_commitdate" &&
      $"l_receiptdate" >= "1994-01-01" && $"l_receiptdate" < "1995-01-01")
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"l_shipmode", $"o_orderpriority")
      .groupBy($"l_shipmode")
      .agg(sum(highPriority($"o_orderpriority")), sum(lowPriority($"o_orderpriority")))
      .sort($"l_shipmode")

    outputDF(res)

  }

}
