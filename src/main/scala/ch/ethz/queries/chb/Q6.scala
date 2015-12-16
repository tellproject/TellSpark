package ch.ethz.queries.chb

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell._
import org.apache.spark.sql.SQLContext

/**
 * Query6
 * 
 * select	sum(ol_amount) as revenue
 * from	orderline
 * where	ol_delivery_d >= '1999-01-01 00:00:00.000000'
 * and ol_delivery_d < '2020-01-01 00:00:00.000000'
 * and ol_quantity between 1 and 100000
 */
class Q6 extends ChQuery {

  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import BufferType._
//    // prepare date selection
    val oSchema = ChTSchema.orderLineSch
    val olQry = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    val oDeliveryIndex = oSchema.getField("ol_delivery_d").index

    val dateSelectionLower = new CNFClause
    dateSelectionLower.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oDeliveryIndex, referenceDate1999)
//    olQry.addSelection(dateSelectionLower)

    val dateSelectionUpper = new CNFClause
    dateSelectionUpper.addPredicate(
      ScanQuery.CmpType.LESS, oDeliveryIndex, referenceDate2020First)
    // as anything satisfy the query, we can leave this filter away
//    olQry.addSelection(dateSelectionUpper)

    // prepare quantity selection
    val oQuantityIndex = oSchema.getField("ol_quantity").index
    val quantitySelectionLower = new CNFClause
    quantitySelectionLower.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oQuantityIndex, PredicateType.create(1: Short))
//    orderLineQuery.addSelection(quantitySelectionLower)

    val quantitySelectionUpper = new CNFClause
    quantitySelectionUpper.addPredicate(
      ScanQuery.CmpType.LESS, oQuantityIndex, PredicateType.create(100: Short))
    // the original benchmark says 100000 which is not a numeric(2)!!
    // as anything satisfy the query, we can leave this filter away
    //orderLineQuery.addSelection(quantitySelectionUpper)

    //todo: push down aggregation!

    // convert an RDDs to a DataFrames
    val orderline = orderLineRdd(tSparkContext, olQry, oSchema).toDF()
    //Do push downs
    val res = orderline
      .filter($"ol_delivery_d" >= 19990101)
      .filter($"ol_delivery_d" < 20200101)
      .filter($"ol_quantity" >= 1)
      .filter($"ol_quantity" <= 10000)
      .agg(sum($"ol_amount"))

//    val res = orderline
    timeCollect(res, 6)
  }
}
