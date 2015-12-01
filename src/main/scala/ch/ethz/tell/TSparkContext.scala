package ch.ethz.tell

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
 * Spark context in charge of sending configuration parameters
 */
class TSparkContext (@transient val conf: SparkConf) extends Serializable{

  /**
   * SparkContext setup
   */
  @transient val sparkContext = new SparkContext(conf)
  // class logger
  val logger = LoggerFactory.getLogger(this.getClass)

  var storageMng: Broadcast[String] = null
  var commitMng: Broadcast[String] = null
  var partNum: Broadcast[Int] = null
  var chSize: Broadcast[Long] = null
  var broadcastTc: Broadcast[Long] = null

  def this(masterUrl: String, appName: String, strMng: String, cmMng: String, pNum: Int, chSz: Long) {

    this(new SparkConf().setMaster(masterUrl).setAppName(appName))
    logger.warn("[%s] SPARK_EXECUTOR_MEMORY_STATUS: %d".format(this.getClass.getSimpleName, sparkContext.getExecutorMemoryStatus))
    storageMng = sparkContext.broadcast(strMng)
    commitMng = sparkContext.broadcast(cmMng)
    partNum = sparkContext.broadcast(pNum)
    chSize = sparkContext.broadcast(chSz)

    TClientFactory.setConf(strMng, cmMng,chSz)

    if (broadcastTc == null) {
      TClientFactory.startTransaction
      broadcastTc = sparkContext.broadcast(TClientFactory.trx.getTransactionId)
      TClientFactory.startTransaction(broadcastTc.value)
      logger.debug("[%s] TransactionId set through SPARK_CONTEXT: %d".format(this.getClass.getSimpleName, broadcastTc.value))
    }
  }

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf
}
