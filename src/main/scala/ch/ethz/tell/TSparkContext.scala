package ch.ethz.tell

import ch.ethz.TellClientFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by marenato on 11.11.15.
 */
class TSparkContext (@transient val conf: SparkConf) extends Serializable{

  /**
   * SparkContext setup
   */
  @transient val sparkContext = new SparkContext(conf)

  var storageMng: Broadcast[String] = null
  var commitMng: Broadcast[String] = null
  var chNumber: Broadcast[Int] = null
  var chSize: Broadcast[Int] = null
  var broadcastTc: Broadcast[Long] = null

  def this(masterUrl: String, appName: String, strMng: String, cmMng: String, chNum: Int, chSz: Int) {

    this(new SparkConf().setMaster(masterUrl).setAppName(appName))
    storageMng = sparkContext.broadcast(strMng)
    commitMng = sparkContext.broadcast(cmMng)
    chNumber = sparkContext.broadcast(chNum)
    chSize = sparkContext.broadcast(chSz)

    TellClientFactory.setConf(strMng, cmMng, chNum,chSz)

    println("==============PRE TRANSACTION =================")
    if (broadcastTc == null) {
      TellClientFactory.startTransaction
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
