package ch.ethz.queries.chb

import ch.ethz.queries.ChQuery
import ch.ethz.tell.{CNFClause, PredicateType, ScanQuery, TSparkContext}

/**
 * Query22
 */
class Q22 extends ChQuery {

  /**
   * select
   *  substr(c_state,1,1) as country, count(*) as numcust, sum(c_balance) as totacctbal
   * from	 customer
   * where substr(c_phone,1,1) in ('1','2','3','4','5','6','7')
   * and c_balance > (
   *     select avg(c_BALANCE)
   *    from customer
   *    where  c_balance > 0.00
   *    and substr(c_phone,1,1) in ('1','2','3','4','5','6','7')
   * )
   * and not exists (
   *    select *
   *    from	orders
   *    where	o_c_id = c_id and o_w_id = c_w_id and o_d_id = c_d_id
   * )
   * group by substr(c_state,1,1)
   * order by substr(c_state,1,1)
   */

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // prepare phone selection
    val cSchema = ChTSchema.customerSch
    val customerQuery = new ScanQuery
    val cPhoneIndex = cSchema.getField("c_phone").index

    val phoneSelection = new CNFClause
    for( d <- 1 to 7){
      phoneSelection.addPredicate(
        ScanQuery.CmpType.LIKE, cPhoneIndex, PredicateType.create(String.valueOf(d)))
    }
//    customerQuery.addSelection(phoneSelection)

    val fcustomer = customerRdd(scc, customerQuery, cSchema).toDF()
      .filter($"c_phone".substr(1,1).isin("1","2","3","4","5","6","7"))
    val order = orderRdd(scc, new ScanQuery, ChTSchema.orderSch).toDF()

    val avg_cbal = fcustomer.filter($"c_balance" > 0).select($"c_balance").agg(avg($"c_balance").as("avg_balance"))
    val res = fcustomer.join(order,
      $"c_id" !== order("o_c_id")
//        && $"c_w_id" !== order("o_w_id")
//        && $"c_d_id" !== order("o_d_id")
    )
    .join(avg_cbal, $"c_balance" > avg_cbal("avg_balance"))//.filter($"c_balance" > avg_cbal("avg_balance"))
    .select($"c_state".substr(1,1).as("country"), $"c_balance")
    .groupBy($"country")
    .agg(count("c_balance").as("numcust"), sum("c_balance").as("totacctbal"))

    timeCollect(res, 22)
    scc.sparkContext.stop()
  }
}
