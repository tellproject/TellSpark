package ch.ethz.tell

import ch.ethz.TScanQuery
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
  var storageNum: Broadcast[Int] = null
  var commitMng: Broadcast[String] = null
  var partNum: Broadcast[Int] = null
  var chSizeSmall: Broadcast[Long] = null
  var chSizeBig: Broadcast[Long] = null
  var chunkNum: Broadcast[Int] = null
  var broadcastTc: Broadcast[Long] = null

  @transient var mainTrx : Transaction = null  // used by driver program

  def this(strMng: String, cmMng: String, pNum: Int,
           chSzSmall: Long, chSzBig: Long, paralScans: Int) {

    this(new SparkConf())
    //logger.warn("[%s] SPARK_EXECUTOR_MEMORY_STATUS: %d".format(this.getClass.getSimpleName, sparkContext.getExecutorMemoryStatus.toString))
    storageMng = sparkContext.broadcast(strMng)
    storageNum = sparkContext.broadcast(strMng.split(";").length)
    commitMng = sparkContext.broadcast(cmMng)
    partNum = sparkContext.broadcast(pNum)
    chSizeSmall = sparkContext.broadcast(chSzSmall)
    chSizeBig= sparkContext.broadcast(chSzBig)
    chunkNum = sparkContext.broadcast(paralScans * storageNum.value)
    logger.warn("end of sparkcontext-constructor, thread-id:" + Thread.currentThread().getId
      + ", spark-context-object-hash: " + this.toString)
  }

  // creates a new transaction with no result buffer attached
  // used mainly to get a new transaction-id
  def startTransaction() = {
    logger.info("[%s] New client created.".format(this.getClass.getSimpleName))
    TStorageConnection.getInstance(commitMng.value, storageMng.value,
        chSizeSmall.value, chSizeBig.value, chunkNum.value)
    mainTrx = Transaction.startTransaction(TStorageConnection.clientManager)
    logger.info("[%s] Started transaction with trxId %d.".format(this.getClass.getSimpleName, mainTrx.getTransactionId))
    if (broadcastTc != null)
       logger.info("[%s] Previous transaction with trxId %d.".format(this.getClass.getSimpleName, broadcastTc.value))
    broadcastTc = sparkContext.broadcast(mainTrx.getTransactionId)
  }

  // gets a transaction object for the given transaction-id.
  // the given client manager must have memory chunks allocated for buffering results
  def getTransaction(trId: Long) = {
    TStorageConnection.getInstance(commitMng.value, storageMng.value,
      chSizeSmall.value, chSizeBig.value, chunkNum.value)
    logger.info("[%s] Getting transaction with trxId %d.".format(this.getClass.getSimpleName, trId))
    Transaction.startTransaction(trId, TStorageConnection.clientManager)
  }

  def startScan(tScanQuery: TScanQuery): ScanIterator = {
    TStorageConnection.getInstance(commitMng.value, storageMng.value,
      chSizeSmall.value, chSizeBig.value, chunkNum.value)
    val trx = getTransaction(broadcastTc.value)
    logger.info("[%s] Starting scan with trxId %d.".format(this.getClass.getName, broadcastTc.value))
    trx.scan(TStorageConnection.scanMemoryManagers(tScanQuery.getScanMemoryManagerIndex()), tScanQuery)
  }

  // make sure this is called on Master only!
  def commitTrx() = {
    mainTrx.commit
  }

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf
}

