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
  var trx : Transaction = null

  //  val clientManager : ClientManager = new ClientManager(commitMng, tellStr, chunkCount, chunkSize);
  var clientManager: ClientManager = null

  def getConnection(): ClientManager = {
    if (clientManager == null) {
      clientManager = new ClientManager(commitMng, storageMng, chNumber, chSize)
    }
    clientManager
  }

  def startTransaction() = {
    trx = Transaction.startTransaction(getConnection)
  }

  def commitTrx() = {
    trx.commit()
  }

  // TODO we should get the number of partitions from tell
  // number of memory regions to be read
  var array = Array.empty[Long]
  // the transactions we need to pay attention to
  val trxId: Long = 0


  def getMemLocations(): Array[Long] = {
    if (array.isEmpty) {
      array = new Array[Long](chNumber)

      val u: Unsafe = getUnsafe()
      val tester: NativeTester = new NativeTester
//      (0 to nPartitions - 1).map(n => {
//        val memAddr: Long = tester.createStruct
//        array(n) = memAddr
//      })
    }
    array
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
