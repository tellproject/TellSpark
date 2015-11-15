package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TRecord, TRDD}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.min
import org.slf4j.{LoggerFactory, Logger}
import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import java.io.File

case class OrderLine(OL_O_ID: Int,
                     OL_D_ID: Short,
                     OL_W_ID: Int,
                     OL_NUMBER: Short,
                     OL_I_ID: Int,
                     OL_SUPPLY_W_ID: Int,
                     OL_DELIVERY_D: Long,
                     OL_QUANTITY: Short,
                     OL_AMOUNT: Double,
                     OL_DIST_INFO: String)

case class Warehouse(W_ID: Int,
                     W_NAME: String,
                     W_STREET_1: String,
                     W_STREET_2: String,
                     W_CITY: String,
                     W_STATE: String,
                     W_ZIP: String,
                     W_TAX: Double,
                     W_YTD: Double)

case class District(D_ID: Short,
                    D_W_ID: Int,
                    D_NAME: String,
                    D_STREET_1: String,
                    D_STREET_2: String,
                    D_CITY: String,
                    D_STATE: String,
                    D_ZIP: String,
                    D_TAX: Double,
                    D_YTD: Double,
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
                    C_CREDIT_LIM: Double,
                    C_DISCOUNT: Double,
                    C_BALANCE: Double,
                    C_YTD_PAYMENT: Double,
                    C_PAYMENT_CNT: Short,
                    C_DELIVERY_CNT: Short,
                    C_DATA: String,
                    C_N_NATIONKEY: Int)

case class History(H_C_ID: Short,
                   H_C_D_ID: Short,
                   H_C_W_ID: Int,
                   H_D_ID: Short,
                   H_W_ID: Int,
                   H_DATE: Long,
                   H_AMOUNT: Double,
                   H_DATA: String)

case class NewOrder(NO_O_ID: Int,
                    NO_D_ID: Short,
                    NO_W_ID: Int)

case class Order(O_ID: Int,
                 O_D_ID: Short,
                 O_W_ID: Int,
                 O_C_ID: Short,
                 O_ENTRY_D: Long,
                 O_CARRIER_ID: Short,
                 O_OL_CNT: Short,
                 O_ALL_LOCAL: Short)

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
                    SU_ACCTBAL: Double,
                    SU_COMMENT: String)

case class Stock(S_I_ID: Int,
                 S_W_ID: Int,
                 S_QUANTITY: Short,
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

case class Item(I_ID: Int,
                I_IM_ID: Short,
                I_NAME: String,
                I_PRICE: Double,
                I_DATA: String)

abstract class ChQuery {

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
  def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String): Unit

  def timeCollect(df: DataFrame, queryNo: Int): Unit = {
    val t0 = System.nanoTime()
    //TODO do we need to get all the tuples? or just count them?
    df.collect().foreach(println)
    val t1 = System.nanoTime()
    logger.info("[Query %d] Elapsed time: %d msecs".format(queryNo, (t1-t0)/1000000))
  }

}

object ChQuery {


  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int, st: String, cm: String, cn:Int, cs:Int, mUrl:String): Unit = {
    assert(queryNo >= 1 && queryNo <= 22, "Invalid query number")
    val m = Class.forName(f"ch.ethz.queries.Q${queryNo}%d").newInstance.asInstanceOf[ {def execute(st:String, cm:String, cn:Int, cs:Int, mUrl:String)}]
    println("=========== pre execute =============")
    val res = m.execute(st, cm, cn, cs, mUrl)
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
    if (qryNum > 0) {
        executeQuery(qryNum, st, cm, cn, cs, masterUrl)
    } else {
      (1 to 22).map(i => executeQuery(i, st, cm, cn, cs, masterUrl))
    }

  }
}
