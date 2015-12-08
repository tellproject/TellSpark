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
  @transient var clientManager: ClientManager = null
  @transient var scanMemoryManagers: Array[ScanMemoryManager] = null

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
  }

  def initializeClientManager() = {
    logger.warn("initialize client manager if necessary")
    if (clientManager == null) {
      logger.warn("before client creation for %s and %s".format(commitMng.value, storageMng.value))
      clientManager = new ClientManager(commitMng.value, storageMng.value)
      logger.warn("after client creation")
    }
  }

  def initializeMemoryManagers() = {
    logger.warn("initialize scan memory manager if necessary")
    if (scanMemoryManagers == null) {
      logger.warn("before scan memory creation")
      scanMemoryManagers = new Array[ScanMemoryManager](2)
      import BufferType._
      scanMemoryManagers(Small) = new ScanMemoryManager(clientManager, chunkNum.value, chSizeSmall.value)
      scanMemoryManagers(Big) = new ScanMemoryManager(clientManager, chunkNum.value, chSizeBig.value)
      logger.warn("after scan memory creation")
    }
  }

  // creates a new transaction with no result buffer attached
  // used mainly to get a new transaction-id
  def startTransaction() = {
    initializeClientManager
    logger.warn("starting transaction")
    mainTrx = Transaction.startTransaction(clientManager)
    logger.info("[%s] New client created.".format(this.getClass.getName))
    logger.info("[%s] Starting transaction with trxId %d.".format(this.getClass.getName, mainTrx.getTransactionId))
    broadcastTc = sparkContext.broadcast(mainTrx.getTransactionId)
  }

  // gets a transaction object for the given transaction-id.
  // the given client manager must have memory chunks allocated for buffering results
  def getTransaction(trId: Long) = {
    initializeClientManager
    initializeMemoryManagers
    logger.warn("getting transaction")
    logger.info("[%s] New client created.".format(this.getClass.getName))
    logger.info("[%s] Starting transaction with trxId %d.".format(this.getClass.getName, trId))
    Transaction.startTransaction(trId, clientManager)
  }

  def startScan(tScanQuery: TScanQuery): ScanIterator = {
    val trx = getTransaction(broadcastTc.value)
    trx.scan(scanMemoryManagers(tScanQuery.getScanMemoryManagerIndex()), tScanQuery)
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
