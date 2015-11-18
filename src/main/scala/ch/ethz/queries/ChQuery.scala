package ch.ethz.queries

import java.util.Calendar

import ch.ethz.TellClientFactory
import ch.ethz.tell.PredicateType.LongType
import ch.ethz.tell.{TSparkContext, ScanQuery, TRecord, TSchema, TRDD}
import org.slf4j.{LoggerFactory}
import org.apache.spark.sql.DataFrame

case class Warehouse(W_ID: Int,
                     W_NAME: String,
                     W_STREET_1: String,
                     W_STREET_2: String,
                     W_CITY: String,
                     W_STATE: String,
                     W_ZIP: String,
                     W_TAX: Int, // numeric (4,4)
                     W_YTD: Long)

// numeric (12,2)

case class District(D_ID: Short,
                    D_W_ID: Short,
                    D_NAME: String,
                    D_STREET_1: String,
                    D_STREET_2: String,
                    D_CITY: String,
                    D_STATE: String,
                    D_ZIP: String,
                    D_TAX: Int, // numeric (4,4)
                    D_YTD: Long, // numeric (12,2)
                    D_NEXT_O_ID: Int)

case class Customer(c_id: Int,
                    c_d_id: Int,
                    c_w_id: Int,
                    c_first: String,
                    c_middle: String,
                    c_last: String,
                    c_street_1: String,
                    c_street_2: String,
                    c_city: String,
                    c_state: String,
                    c_zip: String,
                    c_phone: String,
                    c_since: Long,
                    c_credit: String,
                    c_credit_lim: Long, // numeric (12,2)
                    c_discount: Int, // numeric (4,4)
                    c_balance: Long, // numeric (12,2)
                    c_ytd_payment: Long, // numeric (12,2)
                    c_payment_cnt: Short,
                    c_delivery_cnt: Short,
                    c_data: String,
                    c_n_nationkey: Int)

case class History(H_C_ID: Int,
                   H_C_D_ID: Short,
                   H_C_W_ID: Int,
                   H_D_ID: Short,
                   H_W_ID: Int,
                   H_DATE: Long, // datetime
                   H_AMOUNT: Int, // numeric (6,2)
                   H_DATA: String)

case class NewOrder(no_o_id: Int,
                    no_d_id: Short,
                    no_w_id: Int)

case class Order(O_ID: Int,
                 O_D_ID: Short,
                 O_W_ID: Int,
                 O_C_ID: Short,
                 O_ENTRY_D: Long, // datetime
                 O_CARRIER_ID: Short,
                 O_OL_CNT: Short,
                 O_ALL_LOCAL: Short)

case class OrderLine(ol_o_id: Int,
                     ol_d_id: Short,
                     ol_w_id: Short,
                     ol_number: Short,
                     ol_i_id: Int,
                     ol_supply_w_id: Short,
                     ol_delivery_d: Long, // datetime
                     ol_quantity: Short,
                     ol_amount: Int, // numeric (6,2)
                     ol_dist_info: String)

case class Item(i_id: Int,
                i_im_id: Short,
                i_name: String,
                i_price: Int, // numeric (5,2)
                i_data: String)

case class Stock(s_i_id: Int,
                 s_w_id: Short,
                 s_quantity: Int,
                 s_dist_01: String,
                 s_dist_02: String,
                 s_dist_03: String,
                 s_dist_04: String,
                 s_dist_05: String,
                 s_dist_06: String,
                 s_dist_07: String,
                 s_dist_08: String,
                 s_dist_09: String,
                 s_dist_10: String,
                 s_ytd: Int,
                 s_order_cnt: Short,
                 s_remote_cnt: Short,
                 s_data: String,
                 s_su_suppkey: Short) {
}

case class Nation(n_nationkey: Short,
                  n_name: String,
                  n_regionkey: Short,
                  n_comment: String)

case class Region(r_regionkey: Short,
                  r_name: String,
                  r_comment: String)

case class Supplier(su_suppkey: Short,
                    su_name: String,
                    su_address: String,
                    su_nationkey: Short,
                    su_phone: String,
                    su_acctbal: Long, // numberic (12,2)
                    su_comment: String)

class ChQuery {

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  val logger = LoggerFactory.getLogger(this.getClass)

  // have the reference date as it appears in many places

  var referenceDate1999: LongType = null
  var referenceDate2007: LongType = null
  var referenceDate2010: LongType = null
  var referenceDate2012: LongType = null
  var referenceDate2020First: LongType = null
  var referenceDate2020Second: LongType = null
  val calendar = Calendar.getInstance()

   {
    calendar.set(1999, 1, 1)
    referenceDate1999 = new LongType(calendar.getTimeInMillis)

    calendar.set(2007, 1, 2)
    referenceDate2007 = new LongType(calendar.getTimeInMillis)

    calendar.set(2010, 5, 23, 12, 0)
    referenceDate2010 = new LongType(calendar.getTimeInMillis)

    calendar.set(2012, 1, 2)
    referenceDate2012 = new LongType(calendar.getTimeInMillis)

    calendar.set(2020, 1, 1)
    referenceDate2020First = new LongType(calendar.getTimeInMillis)

    calendar.set(2020, 1, 2)
    referenceDate2020Second = new LongType(calendar.getTimeInMillis)
  }


  // create spark context and set class name as the app name
  //  val sc = new SparkContext(new SparkConf().setAppName("Query: " + className))
  //
  //  convert an RDDs to a DataFrames
  //  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //
  //  import sqlContext.implicits._
  //  import org.apache.spark.sql.functions._

  /**
   * implemented in children classes and hold the actual query
   */
  def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = ???

  def timeCollect(df: DataFrame, queryNo: Int): Unit = {
    val t0 = System.nanoTime()
    var cnt = 0
    val ress = df.collect()
    ress.foreach(r => {
      println("[TTTTTTTTTTTTTTTT]" + r.toString())
      cnt += 1
    })
    val t1 = System.nanoTime()
    logger.info("[Query %d] Elapsed time: %d msecs. map:%d ress:%d".format(queryNo, (t1 - t0) / 1000000, cnt, ress.length))
  }

  def warehouseRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "warehouse", scanQuery, tSchema).map(r => {
      Warehouse(r.getValue("w_id").asInstanceOf[Int],
        r.getValue("w_name").asInstanceOf[String],
        r.getValue("w_street_1").asInstanceOf[String],
        r.getValue("w_street_2").asInstanceOf[String],
        r.getValue("w_city").asInstanceOf[String],
        r.getValue("w_state").asInstanceOf[String],
        r.getValue("w_zip").asInstanceOf[String],
        r.getValue("w_tax").asInstanceOf[Int],
        r.getValue("w_ytd").asInstanceOf[Long]
      )
    })
  }

  def districtRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "district", scanQuery, tSchema).map(r => {
      District(r.getValue("d_id").asInstanceOf[Short],
        r.getValue("d_w_id").asInstanceOf[Short],
        r.getValue("d_name").asInstanceOf[String],
        r.getValue("d_street_1").asInstanceOf[String],
        r.getValue("d_street_2").asInstanceOf[String],
        r.getValue("d_city").asInstanceOf[String],
        r.getValue("d_state").asInstanceOf[String],
        r.getValue("d_zip").asInstanceOf[String],
        r.getValue("d_tax").asInstanceOf[Int],
        r.getValue("d_ytd").asInstanceOf[Long],
        r.getValue("d_next_o_id").asInstanceOf[Int]
      )
    })
  }

  def customerRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "customer", scanQuery, tSchema).map(r => {
      Customer(r.getValue("c_id").asInstanceOf[Int],
        r.getValue("c_d_id").asInstanceOf[Int],
        r.getValue("c_w_id").asInstanceOf[Int],
        r.getValue("c_first").asInstanceOf[String],
        r.getValue("c_middle").asInstanceOf[String],
        r.getValue("c_last").asInstanceOf[String],
        r.getValue("c_street_1").asInstanceOf[String],
        r.getValue("c_street_2").asInstanceOf[String],
        r.getValue("c_city").asInstanceOf[String],
        r.getValue("c_state").asInstanceOf[String],
        r.getValue("c_zip").asInstanceOf[String],
        r.getValue("c_phone").asInstanceOf[String],
        r.getValue("c_since").asInstanceOf[Long],
        r.getValue("c_credit").asInstanceOf[String],
        r.getValue("c_credit_lim").asInstanceOf[Long],
        r.getValue("c_discount").asInstanceOf[Int],
        r.getValue("c_balance").asInstanceOf[Long],
        r.getValue("c_ytd_payment").asInstanceOf[Long],
        r.getValue("c_payment_cnt").asInstanceOf[Short],
        r.getValue("c_delivery_cnt").asInstanceOf[Short],
        r.getValue("c_data").asInstanceOf[String],
        r.getValue("c_n_nationkey").asInstanceOf[Int]
      )
    })
  }

  def historyRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "history", scanQuery, tSchema).map(r => {
      History(r.getValue("h_c_id").asInstanceOf[Int],
        r.getValue("h_c_d_id").asInstanceOf[Short],
        r.getValue("h_c_w_id").asInstanceOf[Short],
        r.getValue("h_d_id").asInstanceOf[Short],
        r.getValue("h_w_id").asInstanceOf[Short],
        r.getValue("h_date").asInstanceOf[Long],
        r.getValue("h_amount").asInstanceOf[Int],
        r.getValue("h_data").asInstanceOf[String]
      )
    })
  }

  def newOrderRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "new_order", new ScanQuery(), tSchema).map(r => {
      NewOrder(r.getValue("no_o_id").asInstanceOf[Int],
        r.getValue("no_d_id").asInstanceOf[Short],
        r.getValue("no_w_id").asInstanceOf[Int]
      )
    })
  }

  def orderRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "order", scanQuery, tSchema).map(r => {
      Order(r.getValue("o_id").asInstanceOf[Int],
        r.getValue("o_d_id").asInstanceOf[Short],
        r.getValue("o_w_id").asInstanceOf[Int],
        r.getValue("o_c_id").asInstanceOf[Short],
        r.getValue("o_entry_d").asInstanceOf[Long],
        r.getValue("o_carrier_id").asInstanceOf[Short],
        r.getValue("o_ol_cnt").asInstanceOf[Short],
        r.getValue("o_all_local").asInstanceOf[Short]
      )
    })
  }

  def orderLineRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "order-line", scanQuery, tSchema).map(r => {
      OrderLine(r.getValue("ol_o_id").asInstanceOf[Int],
        r.getValue("ol_d_id").asInstanceOf[Short],
        r.getValue("ol_w_id").asInstanceOf[Short],
        r.getValue("ol_number").asInstanceOf[Short],
        r.getValue("ol_i_id").asInstanceOf[Int],
        r.getValue("ol_supply_w_id").asInstanceOf[Short],
        r.getValue("ol_delivery_d").asInstanceOf[Long],
        r.getValue("ol_quantity").asInstanceOf[Short],
        r.getValue("ol_amount").asInstanceOf[Int],
        r.getValue("ol_dist_info").asInstanceOf[String]
      )
    })
  }

  def itemRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "item", scanQuery, tSchema).map(r => {
      Item(r.getValue("i_id").asInstanceOf[Int],
        r.getValue("i_im_id").asInstanceOf[Short],
        r.getValue("i_name").asInstanceOf[String],
        r.getValue("i_price").asInstanceOf[Int],
        r.getValue("i_data").asInstanceOf[String]
      )
    })
  }

  def stockRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "stock", scanQuery, tSchema).map(r => {
      Stock(r.getValue("s_i_id").asInstanceOf[Int],
        r.getValue("s_w_id").asInstanceOf[Short],
        r.getValue("s_quantity").asInstanceOf[Int],
        r.getValue("s_dist_01").asInstanceOf[String],
        r.getValue("s_dist_02").asInstanceOf[String],
        r.getValue("s_dist_03").asInstanceOf[String],
        r.getValue("s_dist_04").asInstanceOf[String],
        r.getValue("s_dist_05").asInstanceOf[String],
        r.getValue("s_dist_06").asInstanceOf[String],
        r.getValue("s_dist_07").asInstanceOf[String],
        r.getValue("s_dist_08").asInstanceOf[String],
        r.getValue("s_dist_09").asInstanceOf[String],
        r.getValue("s_dist_10").asInstanceOf[String],
        r.getValue("s_ytd").asInstanceOf[Int],
        r.getValue("s_order_cnt").asInstanceOf[Short],
        r.getValue("s_remote_cnt").asInstanceOf[Short],
        r.getValue("s_data").asInstanceOf[String],
        r.getValue("s_su_suppkey").asInstanceOf[Short]
      )
    })
  }

  def regionRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "region", scanQuery, tSchema).map(r => {
      Region(r.getValue("r_regionkey").asInstanceOf[Short],
        r.getValue("r_name").asInstanceOf[String],
        r.getValue("r_comment").asInstanceOf[String]
      )
    })
  }

  def nationRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "nation", scanQuery, tSchema).map(r => {
      Nation(r.getValue("n_nationkey").asInstanceOf[Short],
        r.getValue("n_name").asInstanceOf[String],
        r.getValue("n_regionkey").asInstanceOf[Short],
        r.getValue("n_comment").asInstanceOf[String])
    })
  }

  def supplierRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "supplier", scanQuery, tSchema).map(r => {
      Supplier(r.getValue("su_suppkey").asInstanceOf[Short],
        r.getValue("su_name").asInstanceOf[String],
        r.getValue("su_address").asInstanceOf[String],
        r.getValue("su_nationkey").asInstanceOf[Short],
        r.getValue("su_phone").asInstanceOf[String],
        r.getValue("su_acctbal").asInstanceOf[Long],
        r.getValue("su_comment").asInstanceOf[String])
    })
  }
}

object ChQuery {



  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int, st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    assert(queryNo >= 1 && queryNo <= 22, "Invalid query number")
    val m = Class.forName(f"ch.ethz.queries.Q${queryNo}%d").newInstance.asInstanceOf[ {def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String)}]
    println("=========== pre execute =============")
    val res = m.execute(st, cm, cn, cs, mUrl)
    println("=========== post execute =============")
  }

  def main(args: Array[String]): Unit = {
    var st = "192.168.0.21:7241"
    var cm = "192.168.0.21:7242"
    var cn = 4
    var cs = 5120000
    var masterUrl = "local[1]"
    var qryNum = 6

    // client properties
    if (args.length >= 4) {
      st = args(0)
      cm = args(1)
      cn = args(2).toInt
      cs = args(3).toInt
      if (args.length == 6) {
        masterUrl = args(4)
        qryNum = args(5).toInt
      } else {
        println("[TELL] Incorrect number of parameters")
        println("[TELL] <strMng> <commitMng> <chunkNum> <chunkSz> <masterUrl> <appName>")
        throw new RuntimeException("Invalid number of arguments")
      }
    }

    TellClientFactory.setConf(st, cm, cn, cs)
    TellClientFactory.getConnection()
    TellClientFactory.startTransaction()
    ChTSchema.init_schem(TellClientFactory.trx)
    TellClientFactory.commitTrx()

    println("***********************************************")
    println("********************q:" + qryNum + "***st:" + st + "***cm:" + cm)
    println("***********************************************")
    if (qryNum > 0) {
      executeQuery(qryNum, st, cm, cn, cs, masterUrl)
    } else {
      (1 to 22).map(i => executeQuery(i, st, cm, cn, cs, masterUrl))
    }

  }
}
