package ch.ethz.queries.tpch

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import ch.ethz.queries.main.scala.TpchQuery

/**
 * TPC-H Query 10
 * Savvas Savvides <ssavvides@us.ibm.com>
 *
 */
class Q10 extends TpchQuery {

  import sqlContext.implicits._

  override def execute(): Unit = {

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val flineitem = lineitem.filter($"l_returnflag" === "R")

    val res = order.filter($"o_orderdate" < "1994-01-01" && $"o_orderdate" >= "1993-10-01")
      .join(customer, $"o_custkey" === customer("c_custkey"))
      .join(nation, $"c_nationkey" === nation("n_nationkey"))
      .join(flineitem, $"o_orderkey" === flineitem("l_orderkey"))
      .select($"c_custkey", $"c_name",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment")
      .groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc)
      .limit(20)

    outputDF(res)

  }

}
