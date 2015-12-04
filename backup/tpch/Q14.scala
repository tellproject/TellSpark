package ch.ethz.queries.tpch

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import ch.ethz.queries.main.scala.TpchQuery

/**
 * TPC-H Query 14
 * Savvas Savvides <ssavvides@us.ibm.com>
 *
 */
class Q14 extends TpchQuery {

  import sqlContext.implicits._

  override def execute(): Unit = {

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    val res = part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= "1995-09-01" && $"l_shipdate" < "1995-10-01")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))

    outputDF(res)

  }

}
