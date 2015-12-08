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
    var st = "192.168.0.21:7241"
    var cm = "192.168.0.21:7242"
    var partNum = 4
    var qryNum = 0
    var chunkSizeSmall = 0x100000L  // 1MB
    var chunkSizeBig = 0x100000000L // 4GB
    var parallelScans = 6

    // client properties
    if (args.length >= 3 && args.length <= 7) {
      st = args(0)
      cm = args(1)
      partNum = args(2).toInt
      if (args.length > 3) {
        qryNum = args(3).toInt
      }
      if (args.length > 4) {
        chunkSizeSmall = args(4).toLong
      }
      if (args.length > 5) {
        chunkSizeBig = args(5).toLong
      }
      if (args.length > 6) {
        parallelScans = args(6).toInt
      }
    } else {
      println("[TELL] Incorrect number of parameters")
      println("[TELL] <strMng> <commitMng> <partNum> [<query-num>] [<small-chunk-size>] [<big-chunk-size>] [<num-parallel-scans>]")
      throw new RuntimeException("Invalid number of arguments")
    }

    val tSparkContext: TSparkContext = new TSparkContext(st, cm, partNum, chunkSizeSmall, chunkSizeBig, parallelScans)
    val sqlContext = new org.apache.spark.sql.SQLContext(tSparkContext.sparkContext)
    tSparkContext.startTransaction()
    ChTSchema.init_schema(tSparkContext.mainTrx)
    tSparkContext.commitTrx()

    logger.warn("[%s] Query %d".format(this.getClass.getName,  qryNum))
    val includeList = List(1)
    if (qryNum > 0) {
      executeQuery(qryNum, tSparkContext, sqlContext)
    } else {
      includeList.map(i => {
        logger.warn("Executing query " + i)
        executeQuery(i, tSparkContext, sqlContext)
      }
      )
    }
  }
}
