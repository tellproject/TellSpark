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
                     W_TAX: Int,    // numeric (4,4)
                     W_YTD: Long)   // numeric (12,2)

case class District(D_ID: Short,
                    D_W_ID: Short,
                    D_NAME: String,
                    D_STREET_1: String,
                    D_STREET_2: String,
                    D_CITY: String,
                    D_STATE: String,
                    D_ZIP: String,
                    D_TAX: Int,     // numeric (4,4)
                    D_YTD: Long,    // numeric (12,2)
                    D_NEXT_O_ID: Int)

case class Customer(C_ID: Int,
                    C_D_ID: Int,
                    C_W_ID: Int,
                    C_FIRST: String,
                    C_MIDDLE: String,
                    C_LAST: String,
                    C_STREET_1: String,
                    C_STREET_2: String,
                    C_CITY: String,
                    C_STATE: String,
                    C_ZIP: String,
                    C_PHONE: String,
                    C_SINCE: Long,
                    C_CREDIT: String,
                    C_CREDIT_LIM: Long,     // numeric (12,2)
                    C_DISCOUNT: Int,        // numeric (4,4)
                    C_BALANCE: Long,        // numeric (12,2)
                    C_YTD_PAYMENT: Long,    // numeric (12,2)
                    C_PAYMENT_CNT: Short,
                    C_DELIVERY_CNT: Short,
                    C_DATA: String,
                    C_N_NATIONKEY: Int)

case class History(H_C_ID: Int,
                   H_C_D_ID: Short,
                   H_C_W_ID: Int,
                   H_D_ID: Short,
                   H_W_ID: Int,
                   H_DATE: Long,      // datetime
                   H_AMOUNT: Int,     // numeric (6,2)
                   H_DATA: String)

case class NewOrder(NO_O_ID: Int,
                    NO_D_ID: Short,
                    NO_W_ID: Int)

case class Order(O_ID: Int,
                 O_D_ID: Short,
                 O_W_ID: Int,
                 O_C_ID: Short,
                 O_ENTRY_D: Long,   // datetime
                 O_CARRIER_ID: Short,
                 O_OL_CNT: Short,
                 O_ALL_LOCAL: Short)

case class OrderLine(OL_O_ID: Int,
                    OL_D_ID: Short,
                    OL_W_ID: Int,
                    OL_NUMBER: Short,
                    OL_I_ID: Int,
                    OL_SUPPLY_W_ID: Int,
                    OL_DELIVERY_D: Long,  // datetime
                    OL_QUANTITY: Short,
                    OL_AMOUNT: Int,       // numeric (6,2)
                    OL_DIST_INFO: String)

case class Item(I_ID: Int,
                I_IM_ID: Short,
                I_NAME: String,
                I_PRICE: Int,             // numeric (5,2)
                I_DATA: String)

case class Stock(S_I_ID: Int,
                 S_W_ID: Short,
                 S_QUANTITY: Int,
                 S_DIST_01: String,
                 S_DIST_02: String,
                 S_DIST_03: String,
                 S_DIST_04: String,
                 S_DIST_05: String,
                 S_DIST_06: String,
                 S_DIST_07: String,
                 S_DIST_08: String,
                 S_DIST_09: String,
                 S_DIST_10: String,
                 S_YTD: Int,
                 S_ORDER_CNT: Short,
                 S_REMOTE_CNT: Short,
                 S_DATA: String,
                 S_SU_SUPPKEY: Int)
{
  def this(S_I_ID: Int, S_W_ID: Short, S_QUANTITY: Int, S_DIST_01: String, S_DIST_02: String, S_DIST_03: String,
           S_DIST_04: String, S_DIST_05: String, S_DIST_06: String, S_DIST_07: String, S_DIST_08: String,
           S_DIST_09: String, S_DIST_10: String, S_YTD: Int, S_ORDER_CNT: Short, S_REMOTE_CNT: Short, S_DATA: String) =
    this(S_I_ID, S_W_ID, S_QUANTITY,  S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07,
      S_DIST_08, S_DIST_09, S_DIST_10, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA, 0)
}

case class Nation(N_NATIONKEY: Short,
                  N_NAME: String,
                  N_REGIONKEY: Short,
                  N_COMMENT: String)

case class Region(R_REGIONKEY: Short,
                  R_NAME: String,
                  R_COMMENT: String)

case class Supplier(SU_SUPPKEY: Short,
                    SU_NAME: String,
                    SU_ADDRESS: String,
                    SU_NATIONKEY: Short,
                    SU_PHONE: String,
                    SU_ACCTBAL: Long,     // numberic (12,2)
                    SU_COMMENT: String)

abstract class ChQuery {

  // have the reference date as it appears in many places

  var referenceDate1999:LongType = null
  var referenceDate2007:LongType = null
  var referenceDate2010:LongType = null
  var referenceDate2012:LongType = null
  var referenceDate2020First:LongType = null
  var referenceDate2020Second:LongType = null

  {
    val calendar = Calendar.getInstance()

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

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  val logger = LoggerFactory.getLogger(ChQuery.getClass)
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
  def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String, chTSchema:ChTSchema): Unit

  def timeCollect(df: DataFrame, queryNo: Int): Unit = {
    val t0 = System.nanoTime()
    var cnt = 0
    val ress = df.collect()
    ress.foreach(r => {
      println("[TTTTTTTTTTTTTTTT]" + r.toString())
      cnt += 1
    })
    val t1 = System.nanoTime()
    logger.info("[Query %d] Elapsed time: %d msecs. map:%d ress:%d".format(queryNo, (t1-t0)/1000000, cnt, ress.length))
  }

  def warehouseRdd(scc:TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
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

  def districtRdd(scc:TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
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

  def customerRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
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

  def historyRdd(scc:TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
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

  def newOrderRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
    new TRDD[TRecord](scc, "new_order", new ScanQuery(), tSchema).map(r => {
      NewOrder(r.getValue("no_o_id").asInstanceOf[Int],
      r.getValue("no_d_id").asInstanceOf[Short],
      r.getValue("no_w_id").asInstanceOf[Int]
      )
    })
  }
  def orderRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
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

  def orderLineRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
    new TRDD[TRecord](scc, "order-line", scanQuery, tSchema).map(r => {
      OrderLine(r.getValue("ol_o_id").asInstanceOf[Int],
      r.getValue("ol_d_id").asInstanceOf[Short],
      r.getValue("ol_e_id").asInstanceOf[Int],
      r.getValue("ol_number").asInstanceOf[Short],
      r.getValue("ol_i_id").asInstanceOf[Int],
      r.getValue("ol_supply_w_id").asInstanceOf[Int],
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

  def stockRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
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
      r.getValue("s_su_suppkey").asInstanceOf[Int]
      )
    })
  }

  def regionRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
    new TRDD[TRecord](scc, "region", scanQuery, tSchema).map(r => {
      Region(r.getValue("r_regionkey").asInstanceOf[Short],
      r.getValue("r_name").asInstanceOf[String],
      r.getValue("r_comment").asInstanceOf[String]
      )
    })
  }

  def nationRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
    new TRDD[TRecord](scc, "nation", scanQuery, tSchema).map(r => {
      Nation(r.getValue("n_nationkey").asInstanceOf[Short],
      r.getValue("n_name").asInstanceOf[String],
      r.getValue("n_regionkey").asInstanceOf[Short],
      r.getValue("n_comment").asInstanceOf[String])
    })
  }

  def supplierRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema:TSchema) = {
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
  def executeQuery(queryNo: Int, st: String, cm: String, cn:Int, cs:Int, mUrl:String, chTSchema:ChTSchema): Unit = {
    assert(queryNo >= 1 && queryNo <= 22, "Invalid query number")
    val m = Class.forName(f"ch.ethz.queries.Q${queryNo}%d").newInstance.asInstanceOf[
      {def execute(st:String, cm:String, cn:Int, cs:Int, mUrl:String, chTSchema:ChTSchema)}]
    println("=========== pre execute =============")
    val res = m.execute(st, cm, cn, cs, mUrl, chTSchema)
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
    val chtSchema = new ChTSchema(TellClientFactory.trx)
    TellClientFactory.commitTrx()

    println("***********************************************")
    println("********************q:" + qryNum + "***st:" + st + "***cm:" + cm)
    println("***********************************************")
    if (qryNum > 0) {
        executeQuery(qryNum, st, cm, cn, cs, masterUrl, chtSchema)
    } else {
      (1 to 22).map(i => executeQuery(i, st, cm, cn, cs, masterUrl, chtSchema))
    }

  }
}
