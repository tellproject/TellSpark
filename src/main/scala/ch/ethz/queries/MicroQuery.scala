package ch.ethz.queries

import ch.ethz.queries.chb.ChTSchema
import ch.ethz.tell.TSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

object MicroQuery {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int, tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    assert(queryNo >= 1 && queryNo <= 22, "Invalid query number")
    tSparkContext.startTransaction()
    val m = Class.forName(f"ch.ethz.queries.micro.Q${queryNo}%d").newInstance.asInstanceOf[{
      def execute(tSparkContext: TSparkContext, sqlContext: SQLContext)}]
    logger.info("[%s] Pre query execution".format(this.getClass.getName))
    val res = m.execute(tSparkContext, sqlContext)
    tSparkContext.commitTrx()
    logger.info("[%s] Post query execution".format(this.getClass.getName))
  }

  def main(args: Array[String]): Unit = {
    // does parameters handling
    ParamHandler.getParams(args)

    val tSparkContext: TSparkContext = new TSparkContext(ParamHandler.st, ParamHandler.cm, ParamHandler.partNum,
      ParamHandler.chunkSizeSmall, ParamHandler.chunkSizeBig, ParamHandler.chunkSizeMedium, ParamHandler.parallelScans)
    val sqlContext = new org.apache.spark.sql.SQLContext(tSparkContext.sparkContext)
    tSparkContext.startTransaction()
    ChTSchema.init_schema(tSparkContext.mainTrx)
    tSparkContext.commitTrx()

    logger.warn("[%s] Query %d".format(this.getClass.getName, ParamHandler.qryNum))
    val includeList = List(1)
    if (ParamHandler.qryNum > 0) {
      executeQuery(ParamHandler.qryNum, tSparkContext, sqlContext)
    } else {
      includeList.map(i => {
        logger.warn("Executing query " + i)
        executeQuery(i, tSparkContext, sqlContext)
      }
      )
    }
  }
}
