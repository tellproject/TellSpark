package ch.ethz.queries.micro

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.queries.chb.ChTSchema
import ch.ethz.tell._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Micro QueryA
 */
class Q1 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {

    import BufferType._
    import sqlContext.implicits._
    import ChTSchema._
    import org.apache.spark.sql.functions._

    val olSchema = ChTSchema.orderLineSch

    // dummy rdd
    // TODO feels like a hack, better way to do this?
//    orderLineRdd(tSparkContext, new TScanQuery("order-line",tSparkContext.partNum.value, Big), orderLineSch).count()

    // prepare date selection
    val dateSelection = new CNFClause
    dateSelection.addPredicate(ScanQuery.CmpType.GREATER, olSchema.getField("ol_delivery_d").index, referenceDate2007)
    val selectionQuery = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    selectionQuery.addSelection(dateSelection)
    val olRdd = orderLineRdd(tSparkContext, selectionQuery, orderLineSch)
//    logDataFrame(this.getClass.getSimpleName, olRdd)
//    timeCollect(olRdd, 1)
    val ol = olRdd.toDF()
    timeCollect(ol, 1)

    // empty query
//    val emptyQry = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
//    val ol2 = orderLineRdd(tSparkContext, emptyQry, orderLineSch).toDF
//    ol2.filter($"ol_delivery_d" >= 20071212)
//    logDataFrame(this.getClass.getSimpleName, ol2)
//    timeCollect(ol2, 1)

    // correcteness test
//    val valsSel = ol.agg(max($"ol_delivery_d").as("max_date"), min($"ol_delivery_d").as("min_date")).collect()
//    val valsSpark = ol2.agg(max($"ol_delivery_d").as("max_date"), min($"ol_delivery_d").as("min_date")).collect()
//    valsSel.map( p => logger.warn("[QUERY %s] Pushdown.%s".format(this.getClass.getSimpleName, p.toString())))
//    valsSpark.map( p => logger.warn("[QUERY %s] SparkFilter.%s".format(this.getClass.getSimpleName, p.toString())))
  }

}
