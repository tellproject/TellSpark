package ch.ethz.tell

import ch.ethz.TellClientFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by marenato on 11.11.15.
 */
class TSparkContext (val conf: SparkConf) {

  /**
   * SparkContext setup
   */
  val sparkContext = new SparkContext(conf)

  var storageMng: String = ""
  var commitMng: String = ""
  var chNumber: Int = 0
  var chSize: Int = 0
  var broadcastTc: Broadcast[Long] = null

  def this(masterUrl: String, appName: String, strMng: String, cmMng: String, chNum: Int, chSz: Int) {
    this(new SparkConf().setMaster(masterUrl).setAppName(appName))
    TellClientFactory.storageMng = strMng
    TellClientFactory.commitMng = cmMng
    TellClientFactory.chNumber = chNum
    TellClientFactory.chSize = chSz

    println("==============PRE TRANSACTION =================")
    if (broadcastTc == null) {
      TellClientFactory.startTransaction()
      broadcastTc = sparkContext.broadcast(TellClientFactory.trx.getTransactionId)
      TellClientFactory.startTransaction(broadcastTc.value)
    }
//    val trxId =
    println("==============POST TRANSACTION=================")
  }

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf
}
