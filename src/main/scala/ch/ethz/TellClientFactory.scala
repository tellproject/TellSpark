package ch.ethz

import java.lang.reflect.Field
import ch.ethz.tell.ClientManager
import ch.ethz.tell.Transaction

import sun.misc.Unsafe

/**
 * Object mocking the actual tell client
 */
object TellClientFactory {
  var commitMng: String = ""
  var storageMng: String = ""
  var chNumber = 0
  var chSize = 0
  // TODO we should use it properly
  var trx : Transaction = null

  //  val clientManager : ClientManager = new ClientManager(commitMng, tellStr, chunkCount, chunkSize);
  var clientManager: ClientManager = null

  def getConnection(): ClientManager = {
    if (clientManager == null) {
      println("================= PRE CLIENT ==============")
      println("=================" + toString + "==============")
      clientManager = new ClientManager(commitMng, storageMng, chNumber, chSize)
      println("================= POST CLIENT ==============")
    }
    clientManager
  }

  def startTransaction() = {
    clientManager = getConnection
    println("=========== ClientManagerPointer ======= " + clientManager.getImplPtr)
//    println("=========== ClientManagerPointer ======= " + clientManager.getScanMemoryManagerPtr)
    trx = Transaction.startTransaction(getConnection)
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
