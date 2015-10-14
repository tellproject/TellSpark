package ch.ethz

import org.apache.spark.{SparkConf, SparkContext}
import ch.ethz.tell.{ScanQuery, Schema}

/**
 */
object Experimental {

  val conf = new SparkConf()
  val sc = new SparkContext(conf)

  def main(args : Array[String]) {
    var st = "192.168.0.11:7241"
    var cm = "192.168.0.11:7242"
    var cn = 4
    var cs = 5120000

    // client properties
    if (args.length == 4) {
      st = args(0)
      cm = args(1)
      cn = args(2).toInt
      cs = args(3).toInt
    }

    TellClientFactory.storageMng = st
    TellClientFactory.commitMng = cm
    TellClientFactory.chNumber = cn
    TellClientFactory.chSize = cs

    println("[TELL] PARAMETERS USED: " + TellClientFactory.toString())

    // schema to be read
    val sch: TellSchema = new TellSchema()

    sch.addField(Schema.FieldType.INT, "number", true)
    sch.addField(Schema.FieldType.TEXT, "text1", true)
    sch.addField(Schema.FieldType.BIGINT, "largenumber", true)
    sch.addField(Schema.FieldType.TEXT, "text2", true)

    val tblName = "testTable"
    // rdd creation
    val tellRdd = new TellRDD[Customer](sc, tblName, new ScanQuery(), sch)
    tellRdd.map(println(_))
    val result = tellRdd.collect()
    println("[TUPLES] %d".format(result.length))
  }
}
