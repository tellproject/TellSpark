package ch.ethz.queries.micro

import ch.ethz.TScanQuery
import ch.ethz.queries.{Nation, ChQuery}
import ch.ethz.queries.chb.ChTSchema
import ch.ethz.tell._
import org.apache.spark.sql.SQLContext

/**
 * Micro QueryA
 */
class Q2 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {

    // test projection

    import BufferType._
    import ChTSchema._
    import sqlContext.implicits._

    val rSchema = ChTSchema.regionSch
    val projectionQuery = new TScanQuery("region", tSparkContext.partNum.value, Small)
    projectionQuery.addProjection(rSchema.createProjection("r_regionkey"))
    projectionQuery.addProjection(rSchema.createProjection("r_name"))
    val resultSchema = new TSchema(projectionQuery.getResultSchema)
    val olRdd = new TRDD[TRecord](tSparkContext, projectionQuery, resultSchema).map(r => {
      println(r.getValue("r_regionkey").asInstanceOf[Short])
      println(r.getValue("r_name").asInstanceOf[String])
    })
//    logDataFrame(this.getClass.getSimpleName, olRdd)
    timeCollect(olRdd, 1)
    val regQry = new TScanQuery("region", tSparkContext.partNum.value, Small)
    val olRdd2 = regionRdd(tSparkContext, regQry, ChTSchema.regionSch)
    val res = olRdd2.map( r => {
      println(r.r_name)
      println(r.r_name)
    })
//    val ol = olRdd.toDF()
    timeCollect(res, 1)
  }

}
