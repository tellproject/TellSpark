package ch.ethz.queries

import ch.ethz.queries.chb.ChTSchema
import ch.ethz.tell.TClientFactory
import org.slf4j.LoggerFactory

object MicroQuery {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int, st: String, cm: String, cn: Int, cs: Long, mUrl: String): Unit = {
    assert(queryNo >= 'A' && queryNo <= 'Z', "Invalid query id")
    val m = Class.forName(f"ch.ethz.queries.micro.Q${queryNo}%d").newInstance.asInstanceOf[ {def execute(st: String, cm: String, cn: Int, cs: Long, mUrl: String)}]
    logger.info("[%s] Pre query execution".format(this.getClass.getName))
    val res = m.execute(st, cm, cn, cs, mUrl)
    logger.info("[%s] Post query execution".format(this.getClass.getName))
  }

  def main(args: Array[String]): Unit = {
    var st = "192.168.0.21:7241"
    var cm = "192.168.0.21:7242"
    var cn = 4
    var cs = 5120000L
    var masterUrl = "local[1]"
    var qryNum = 1

    // client properties
    if (args.length >= 4) {
      st = args(0)
      cm = args(1)
      cn = args(2).toInt
      cs = args(3).toLong
      if (args.length == 6) {
        masterUrl = args(4)
        qryNum = args(5).toInt
      } else {
        println("[TELL] Incorrect number of parameters")
        println("[TELL] <strMng> <commitMng> <chunkNum> <chunkSz> <masterUrl> <appName>")
        throw new RuntimeException("Invalid number of arguments")
      }
    }

    TClientFactory.setConf(st, cm, cn, cs)
    TClientFactory.getConnection()
    TClientFactory.startTransaction()
    ChTSchema.init_schem(TClientFactory.trx)
    TClientFactory.commitTrx()

    logger.warn("[%s] Query %d: %s".format(this.getClass.getName,  qryNum, TClientFactory.toString ))
//    val excludeList = List(16,20,21)
    //    val excludeList = List(16,20,21
//    val includeList = List(1,4,6,7,11,17,18,22)
//    if (qryNum > 0) {
      executeQuery(qryNum, st, cm, cn, cs, masterUrl)
//    } else {
//      includeList.map(i => {
//        logger.warn("Executig query " + i)
//        executeQuery(i, st, cm, cn, cs, masterUrl)
//      }
//      )
//    }
  }
}
