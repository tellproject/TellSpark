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
  var chNumber: Broadcast[Int] = null
  var chSize: Broadcast[Int] = null
  var broadcastTc: Broadcast[Long] = null

  def this(masterUrl: String, appName: String, strMng: String, cmMng: String, chNum: Int, chSz: Int) {

    this(new SparkConf().setMaster(masterUrl).setAppName(appName))
    storageMng = sparkContext.broadcast(strMng)
    commitMng = sparkContext.broadcast(cmMng)
    chNumber = sparkContext.broadcast(chNum)
    chSize = sparkContext.broadcast(chSz)

    TClientFactory.setConf(strMng, cmMng, chNum,chSz)

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
