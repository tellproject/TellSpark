package ch.ethz.queries.chb

import ch.ethz.queries.ChQuery
import ch.ethz.tell.PredicateType.IntType
import ch.ethz.tell.{CNFClause, PredicateType, ScanQuery, TSparkContext}

/**
 * Query 19
 */
class Q19 extends ChQuery {

  /**
   * select	sum(ol_amount) as revenue
   * from	orderline, item
   * where
   *    ( ol_i_id = i_id
   *      and i_data like '%a'
   *      and ol_quantity >= 1
   *      and ol_quantity <= 10
   *      and i_price between 1 and 400000
   *      and ol_w_id in (1,2,3)
   *      ) or (
   *      ol_i_id = i_id
   *      and i_data like '%b'
   *      and ol_quantity >= 1
   *      and ol_quantity <= 10
   *      and i_price between 1 and 400000
   *      and ol_w_id in (1,2,4)
   *      ) or (
   *      ol_i_id = i_id
   *      and i_data like '%c'
   *      and ol_quantity >= 1
   *      and ol_quantity <= 10
   *      and i_price between 1 and 400000
   *      and ol_w_id in (1,5,3)
   *   )
   */

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Long, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

//    val aa = udf { (x: String) => x.matches("a") }
//    val bb = udf { (x: String) => x.matches("b") }
//    val cc = udf { (x: String) => x.matches("c") }

    // prepare quantity selection
    val oSchema = ChTSchema.orderLineSch
    val orderLineQuery = new ScanQuery
    val oQuantityIndex = oSchema.getField("ol_quantity").index

    val quantitySelectionLower = new CNFClause
    quantitySelectionLower.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oQuantityIndex, PredicateType.create(1: Short))
    orderLineQuery.addSelection(quantitySelectionLower)

    val quantitySelectionUpper = new CNFClause
    quantitySelectionUpper.addPredicate(
      ScanQuery.CmpType.LESS_EQUAL, oQuantityIndex, PredicateType.create(10: Short))
    orderLineQuery.addSelection(quantitySelectionUpper)

    // prepare filter on ol_w_id (this is actually pre-filtering, still has to be tested on join)
    val oWIdIndex = oSchema.getField("ol_w_id").index

    val wIdSelectionLower = new CNFClause
    wIdSelectionLower.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oWIdIndex, PredicateType.create(1: Short))
    orderLineQuery.addSelection(wIdSelectionLower)

    val wIdSelectionUpper = new CNFClause
    wIdSelectionUpper.addPredicate(
      ScanQuery.CmpType.LESS_EQUAL, oWIdIndex, PredicateType.create(5: Short))
    orderLineQuery.addSelection(wIdSelectionLower)

    // prepare price selection
    val iSchema = ChTSchema.itemSch
    val itemQuery = new ScanQuery
    val iPriceIndex = iSchema.getField("i_price").index

    val iPriceSelectionLower = new CNFClause
    iPriceSelectionLower.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, iPriceIndex, new IntType(100))
    itemQuery.addSelection(iPriceSelectionLower)

    val iPriceSelectionUpper = new CNFClause
    iPriceSelectionUpper.addPredicate(
      ScanQuery.CmpType.LESS_EQUAL, iPriceIndex, new IntType(4000000))
    itemQuery.addSelection(iPriceSelectionUpper)

    val ol = orderLineRdd(scc, orderLineQuery, oSchema)
    val it = itemRdd(scc, itemQuery, iSchema)
    val forderline = ol.toDF()
//      .filter($"ol_quantity" >= 1 && $"ol_quantity" <= 10)
    val fitem = it.toDF()
//      .filter($"i_price" >= 100 && $"i_price" <= 4000000)
    val res = forderline.join(fitem, $"ol_i_id" === $"i_id")
    .filter(
        (($"i_data".like("a")) && $"ol_w_id".isin(1, 2, 3)) ||
        (($"i_data".like("b")) && $"ol_w_id".isin(1, 2, 4)) ||
        (($"i_data".like("c")) && $"ol_w_id".isin(1, 5, 3)))
    .select($"ol_amount")
    .agg(sum($"ol_amount").as("revenue"))

    timeCollect(res, 19)
    scc.sparkContext.stop()
  }

}
