package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TRecord, TRDD}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.min
import scala.reflect.runtime.universe
import org.apache.spark.sql.DataFrame
import java.io.File

case class OrderLine(
                      OL_O_ID: Int,
                      OL_D_ID: Short,
                      OL_W_ID: Int,
                      OL_NUMBER: Short,
                      OL_I_ID: Int,
                      OL_SUPPLY_W_ID: Int,
                      OL_DELIVERY_D: Long,
                      OL_QUANTITY: Short,
                      OL_AMOUNT: Double,
                      OL_DIST_INFO: String)

case class Customer(
                     c_custkey: Int,
                     c_name: String,
                     c_address: String,
                     c_nationkey: Int,
                     c_phone: String,
                     c_acctbal: Double,
                     c_mktsegment: String,
                     c_comment: String)

case class Lineitem(
                     l_orderkey: Int,
                     l_partkey: Int,
                     l_suppkey: Int,
                     l_linenumber: Int,
                     l_quantity: Double,
                     l_extendedprice: Double,
                     l_discount: Double,
                     l_tax: Double,
                     l_returnflag: String,
                     l_linestatus: String,
                     l_shipdate: String,
                     l_commitdate: String,
                     l_receiptdate: String,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String)

case class Nation(
                   n_nationkey: Int,
                   n_name: String,
                   n_regionkey: Int,
                   n_comment: String)

case class Order(
                  o_orderkey: Int,
                  o_custkey: Int,
                  o_orderstatus: String,
                  o_totalprice: Double,
                  o_orderdate: String,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: Int,
                  o_comment: String)

case class Part(
                 p_partkey: Int,
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Int,
                 p_container: String,
                 p_retailprice: Double,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Int,
                     ps_suppkey: Int,
                     ps_availqty: Int,
                     ps_supplycost: Double,
                     ps_comment: String)

case class Region(
                   r_regionkey: Int,
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Int,
                     s_name: String,
                     s_address: String,
                     s_nationkey: Int,
                     s_phone: String,
                     s_acctbal: Double,
                     s_comment: String)


abstract class ChQuery {

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  // create spark context and set class name as the app name
  val sc = new SparkContext(new SparkConf().setAppName("Query: " + className))

  // convert an RDDs to a DataFrames
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(): Unit

  def timeCollect(df: DataFrame): Unit = {
      df.collect().foreach(println)
  }
}

object ChQuery {

  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int): Unit = {
    assert(queryNo >= 1 && queryNo <= 22, "Invalid query number")
    Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[{ def execute }].execute
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 1)
      executeQuery(args(0).toInt)
    else
      throw new RuntimeException("Invalid number of arguments")
  }
}
