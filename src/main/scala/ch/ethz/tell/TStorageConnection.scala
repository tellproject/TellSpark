package ch.ethz.tell

import org.slf4j.LoggerFactory

object TStorageConnection {

  val logger = LoggerFactory.getLogger(this.getClass)
  var clientManager: ClientManager = null
  var scanMemoryManagers: Array[ScanMemoryManager] = null

  private def initializeClientManager(commitMng: String, storageMng: String) = synchronized {
    logger.warn("initialize client manager if necessary, thread-id:" + Thread.currentThread().getId
      + ", spark-context-object-hash: " + this.toString)
    if (clientManager == null) {
      logger.warn("before client creation for %s and %s".format(commitMng, storageMng))
      clientManager = new ClientManager(commitMng, storageMng)
      logger.warn("after client creation")
    }
  }

  private def initializeMemoryManagers(chSizeSmall: Long, chSizeBig: Long, chunkNum: Int) = synchronized {
    logger.warn("initialize scan memory manager if necessary, thread-id:" + Thread.currentThread().getId
      + ", spark-context-object-hash: " + this.toString)
    if (scanMemoryManagers == null) {
      logger.warn("before scan memory creation")
      scanMemoryManagers = new Array[ScanMemoryManager](2)
      import BufferType._
      scanMemoryManagers(Small) = new ScanMemoryManager(clientManager, chunkNum, chSizeSmall)
      scanMemoryManagers(Big) = new ScanMemoryManager(clientManager, chunkNum, chSizeBig)
      logger.warn("after scan memory creation")
    }
  }

  def getInstance(commitMng: String, storageMng: String, chSizeSmall: Long, chSizeBig: Long, chunkNum: Int): Unit = {
    initializeClientManager(commitMng, storageMng)
    initializeMemoryManagers(chSizeSmall, chSizeBig, chunkNum)
    // return a reference to "this" object
  }

}
