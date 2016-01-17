package ch.ethz.tell

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkContext}

object TellConnection extends Logging {
  var clientManagerInstance: ClientManager = null

  var memoryManagerSmallInstance: ScanMemoryManager = null

  var memoryManagerBigInstance: ScanMemoryManager = null

  def init(commitManager: String, storageManager: String, chunkCount: Int, chunkSizeSmall: Long,
      chunkSizeBig: Long): Unit = {
    if (memoryManagerBigInstance == null) {
      this.synchronized {
        if (memoryManagerBigInstance == null) {
          logInfo(s"Initializing client manager [commitManager = ${commitManager}, storageManager = ${storageManager}]")
          clientManagerInstance = new ClientManager(commitManager, storageManager)

          logInfo(s"Initializing small memory manager [count = ${chunkCount} size = ${chunkSizeSmall}]")
          //memoryManagerSmallInstance = new ScanMemoryManager(clientManagerInstance, chunkCount, chunkSizeSmall)

          logInfo(s"Initializing big memory manager [count = ${chunkCount} size = ${chunkSizeBig}]")
          memoryManagerBigInstance = new ScanMemoryManager(clientManagerInstance, chunkCount, chunkSizeBig)
        }
      }
    }
  }
}

class TellConnection(commitManager: String,
                     storageManager: String,
                     chunkCount: Int,
                     chunkSizeSmall: Long,
                     chunkSizeBig: Long) {
  TellConnection.init(commitManager, storageManager, chunkCount, chunkSizeSmall, chunkSizeBig)

  val clientManager = TellConnection.clientManagerInstance

  //val memoryManagerSmall = TellConnection.memoryManagerSmallInstance

  val memoryManagerBig = TellConnection.memoryManagerBigInstance

  def startTransaction(): Transaction = {
    Transaction.startTransaction(clientManager)
  }

  def startTransaction(transactionId: Long): Transaction = {
    Transaction.startTransaction(transactionId, clientManager)
  }
}

/**
 * Spark context in charge of sending configuration parameters
 */
class TellContext(sc: SparkContext) extends SQLContext(sc) with Logging {

  logInfo("Create TellContext")

  @transient
  val tellConf = sc.getConf

  val commitManager = tellConf.get("spark.sql.tell.commitmanager")
  val storageManager = tellConf.get("spark.sql.tell.storagemanager")
  val partitionShift = {
    val storageCount = storageManager.split(";").length
    Math.log(storageCount).toInt
  }
  val chunkCount = tellConf.getInt("spark.sql.tell.chunkCount", 8)
  val chunkSizeSmall = tellConf.getLong("spark.sql.tell.chunkSizeSmall", 4L * 1024L)
  val chunkSizeBig = tellConf.getLong("spark.sql.tell.chunkSizeBig", 4L * 1024L * 1024L)

  @transient
  lazy val connection = new TellConnection(commitManager, storageManager, chunkCount, chunkSizeSmall, chunkSizeBig)

  @transient
  var transaction: Transaction = null

  def transactionId = {
    if (transaction == null) {
      throw new RuntimeException("No transaction is active")
    }
    transaction.getTransactionId
  }

  def startTransaction(): Unit = {
    if (transaction != null) {
      throw new RuntimeException("Another transaction is already active")
    }
    logInfo("Starting transaction")
    transaction = connection.startTransaction()
    logInfo(s"Started transaction [transaction = ${transaction.getTransactionId}]")
  }

  def commitTransaction(): Unit = {
    if (transaction == null) {
      throw new RuntimeException("No transaction is active")
    }
    logInfo(s"Committing transaction [transaction = ${transaction.getTransactionId}]")
    if (!transaction.commit()) {
      throw new RuntimeException("Unable to commit transaction")
    }
    transaction = null
    logInfo("Committed transaction")
  }
}
