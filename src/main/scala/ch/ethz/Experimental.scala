package ch.ethz

import org.apache.spark.{SparkConf, SparkContext}
import ch.ethz.tell.{ClientManager, ScanQuery, Schema}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf
/**
 */
object Experimental {

  val conf = new SparkConf()//.setMaster("local[2]").setAppName("Test")
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
    sch.addField(Schema.FieldType.TEXT, "text1", true)
    sch.addField(Schema.FieldType.BIGINT, "largenumber", true)
    sch.addField(Schema.FieldType.TEXT, "text2", true)

    val tblName = "testTable"
    // rdd creation
    val tellRdd = new TellRDD[Customer](sc, tblName, new ScanQuery(), sch)
//    val tellRdd = new TellRDD[Customer](sc, tblName, new ScanQuery(), null)
    println("=============MAPPING==============")
    val grouped = tellRdd.filter(record => record.getcFirst() > "2007")
      .groupBy(record => record.getcId()).sortByKey().map( p => {
      val idd = p._1
      val it = p._2.iterator
      var s1 = 0
      var s2 = 0
      var cnt = 0
      while(it.hasNext) {
       val cus = it.next()
        s1 += cus.getdId()
        s2 += cus.getwId()
        cnt += 1
      }
      (idd, s1, s2, s1/cnt, s2/cnt)
    })

    /*
    select   ol_number,
	 sum(ol_quantity) as sum_qty,
	 sum(ol_amount) as sum_amount,
	 avg(ol_quantity) as avg_qty,
	 avg(ol_amount) as avg_amount,
	 count(*) as count_order
from	 orderline
where	 ol_delivery_d > '2007-01-02 00:00:00.000000'
group by ol_number order by ol_number
     */
    println("=============COLLECTING==============")
    tellRdd.collect()
  }

}
