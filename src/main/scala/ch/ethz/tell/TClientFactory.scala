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
  var trx : Transaction = null
  var trxId : Long = 0L

  def startTransaction() = {
    //trx = Transaction.startTransaction(getConnection)
    trx = Transaction.startTransaction(new ClientManager(commitMng, storageMng, chNumber, chSize))
    logger.info("[%s] New client created.".format(this.getClass.getName))
    trxId = trx.getTransactionId
    logger.info("[%s] Starting transaction with trxId %d.".format(this.getClass.getName, trxId))
  }

  def startTransaction(trId: Long) = {
    //trx = Transaction.startTransaction(trId, getConnection)
    trx = Transaction.startTransaction(trId, new ClientManager(commitMng, storageMng, chNumber, chSize))
    logger.info("[%s] New client created.".format(this.getClass.getName))
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
  def getUnsafe(): sun.misc.Unsafe = {
    val singleoneInstanceField: java.lang.reflect.Field = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    singleoneInstanceField.setAccessible(true)
    singleoneInstanceField.get(null).asInstanceOf[sun.misc.Unsafe]
  }

}
