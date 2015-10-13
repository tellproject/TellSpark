package ch.ethz

import org.apache.spark.{SparkConf, SparkContext}
import ch.ethz.tell.{ClientManager, ScanQuery, Schema}

/**
 */
object Experimental {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
  val sc = new SparkContext(conf)

  def main(args : Array[String]) {
    // client properties
    TellClientFactory.storageMng = "192.168.0.11:7241"
    TellClientFactory.commitMng = "192.168.0.11:7242"
    TellClientFactory.chNumber = 4
    TellClientFactory.chSize = 5120000

    // schema to be read
//    val cm = new ClientManager(TellClientFactory.storageMng,TellClientFactory.commitMng, TellClientFactory.chNumber, TellClientFactory.chSize)
    val sch: TellSchema = new TellSchema()

    sch.addField(Schema.FieldType.INT, "number", true)
//    sch.addField(Schema.FieldType.TEXT, "text1", true)
//    sch.addField(Schema.FieldType.BIGINT, "largenumber", true)
//    sch.addField(Schema.FieldType.TEXT, "text2", true)

    val tblName = "testTable"
    // rdd creation
//    val tellRdd = new TellRDD[Customer](sc, tblName, new ScanQuery(), sch)
    val tellRdd = new TellRDD[Customer](sc, tblName, new ScanQuery(), null)
    println("=============")
    tellRdd.map(println(_))
    tellRdd.collect()
  }
}
