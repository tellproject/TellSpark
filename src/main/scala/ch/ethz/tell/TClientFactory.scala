package ch.ethz.tell

import org.slf4j.LoggerFactory

/**
 * Object wrapping actual Tell client
 */
object TClientFactory {

  val logger = LoggerFactory.getLogger(this.getClass)

  def setConf(strMng: String, cmMng: String, chSz: Long) = {
    storageMng = strMng
    commitMng = cmMng
    chNumber = strMng.split(";").length
    chSize = chSz
    logger.info("[%s] TellStore configured to: %s".format(this.getClass.getName, this.toString()))
  }

  var commitMng: String = ""
  var storageMng: String = ""
  var chNumber = 0
  var chSize = 0L
  // TODO we should use it properly
  var mainTrx : Transaction = null  // used by driver program

  // creates a new transaction with only a small dummy buffer
  // used mainly to get a new transaction-id
  def startTransaction() = {
    mainTrx = Transaction.startTransaction(new ClientManager(commitMng, storageMng, 1, 64))
    logger.info("[%s] New client created.".format(this.getClass.getName))
    logger.info("[%s] Starting transaction with trxId %d.".format(this.getClass.getName, mainTrx.getTransactionId))
  }

  // gets a transaction object for the given transaction-id. Allocates necessary result buffers.
  def getTransaction(trId: Long) = {
    logger.info("[%s] New client created.".format(this.getClass.getName))
    logger.info("[%s] Starting transaction with trxId %d.".format(this.getClass.getName, trId))
    Transaction.startTransaction(trId, new ClientManager(commitMng, storageMng, chNumber, chSize))
  }

  def commitTrx() = {
    mainTrx.commit
  }

  def getMainTrxId() = {
    mainTrx.getTransactionId
  }

  override def toString() : String = {
    val sb = new StringBuilder
    sb.append("{commitMng:").append(commitMng).append(",")
    sb.append("storageMng:").append(storageMng).append(",")
    sb.append("chNumber:").append(chNumber).append(",")
    sb.append("chSize:").append(chSize).append("}")
    sb.toString()
  }
  def getUnsafe(): sun.misc.Unsafe = {
    val singleoneInstanceField: java.lang.reflect.Field = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    singleoneInstanceField.setAccessible(true)
    singleoneInstanceField.get(null).asInstanceOf[sun.misc.Unsafe]
  }

}
