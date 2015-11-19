package ch.ethz

import java.lang.reflect.Field
import ch.ethz.tell.ClientManager
import ch.ethz.tell.Transaction
import org.slf4j.LoggerFactory

import sun.misc.Unsafe

/**
 * Object mocking the actual tell client
 */
object TellClientFactory {

  val logger = LoggerFactory.getLogger(this.getClass)

  def setConf(strMng: String, cmMng: String, chNum: Int, chSz: Int) = {
    storageMng = strMng
    commitMng = cmMng
    chNumber = chNum
    chSize = chSz
    logger.info("[%s] TellStore configured to: %s".format(this.getClass.getName, this.toString()))
  }

  var commitMng: String = ""
  var storageMng: String = ""
  var chNumber = 0
  var chSize = 0
  // TODO we should use it properly
  var trx : Transaction = null
  var trxId : Long = 0L

  var clientManager: ClientManager = null

  def getConnection(): ClientManager = {
    //TODO move the client creation somewhere else?
    if (clientManager == null) {
      logger.info("[%s] ClientManager is about to be created.".format(this.getClass.getName))
      clientManager = new ClientManager(commitMng, storageMng, chNumber, chSize)
      logger.info("[%s] ClientManager has been created.".format(this.getClass.getName))
    }
    clientManager
  }

  def startTransaction() = {
    trx = Transaction.startTransaction(getConnection)
    trxId = trx.getTransactionId
    logger.info("[%s] Starting transaction with trxId %d.".format(this.getClass.getName, trxId))
  }

  def startTransaction(trId: Long) = {
    trx = Transaction.startTransaction(trId, getConnection)
    trxId = trx.getTransactionId
    logger.info("[%s] Starting transaction with trxId %d.".format(this.getClass.getName, trxId))
  }

  def commitTrx() = {
    trx.commit()
  }

  override def toString() : String = {
    val sb = new StringBuilder
    sb.append("{commitMng:").append(commitMng).append(",")
    sb.append("storageMng:").append(storageMng).append(",")
    sb.append("chNumber:").append(chNumber).append(",")
    sb.append("chSize:").append(chSize).append("}")
    sb.toString()
  }
  def getUnsafe(): Unsafe = {
    val singleoneInstanceField: Field = classOf[Unsafe].getDeclaredField("theUnsafe")
    singleoneInstanceField.setAccessible(true)
    singleoneInstanceField.get(null).asInstanceOf[Unsafe]
  }

}
