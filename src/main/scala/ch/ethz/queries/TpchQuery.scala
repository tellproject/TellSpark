package ch.ethz.queries

package main.scala

import ch.ethz.queries.chb.ChTSchema
import ch.ethz.queries.tpch.TpchTSchema
import ch.ethz.tell._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame

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

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <ssavvides@us.ibm.com>
 *
 */
abstract class TpchQuery {

  // read files from local FS
  // val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/dbgen"

  // read from hdfs
  val INPUT_DIR: String = "/dbgen"

  // if set write results to hdfs, if null write to stdout
  val OUTPUT_DIR: String = "/tpch"

  // get the name of the class excluding dollar signs and package
  val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

  // create spark context and set class name as the app name
  val sc = new SparkContext(new SparkConf().setAppName("TPC-H " + className))

  // convert an RDDs to a DataFrames
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  def customerRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "customer", scanQuery, tSchema).map(r => {
      Customer(
        r.getValue("c_custkey").asInstanceOf[Int],
        r.getValue("c_name").asInstanceOf[String],
        r.getValue("c_address").asInstanceOf[String],
        r.getValue("c_nationkey").asInstanceOf[Int], //TODO Tell TPCC was short
        r.getValue("c_phone").asInstanceOf[String],
        r.getValue("c_acctbal").asInstanceOf[Double], //TODO Tell TPCC was long
        r.getValue("c_mktsegment").asInstanceOf[String],
        r.getValue("c_comment").asInstanceOf[String])
    })
  }

  def lineItemRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "lineitem", scanQuery, tSchema).map(r => {
      Lineitem(
        r.getValue("l_orderkey").asInstanceOf[Int],
        r.getValue("l_partkey").asInstanceOf[Int],
        r.getValue("l_suppkey").asInstanceOf[Int],
        r.getValue("l_linenumber").asInstanceOf[Int],
        r.getValue("l_quantity").asInstanceOf[Double],
        r.getValue("l_extendedprice").asInstanceOf[Double],
        r.getValue("l_discount").asInstanceOf[Double],
        r.getValue("l_tax").asInstanceOf[Double],
        r.getValue("l_returnflag").asInstanceOf[String],
        r.getValue("l_linestatus").asInstanceOf[String],
        r.getValue("l_shipdate").asInstanceOf[String],
        r.getValue("l_commitdate").asInstanceOf[String],
        r.getValue("l_receiptdate").asInstanceOf[String],
        r.getValue("l_shipinstruct").asInstanceOf[String],
        r.getValue("l_shipmode").asInstanceOf[String],
        r.getValue("l_comment").asInstanceOf[String])
    })
  }

  def nationRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "nation", scanQuery, tSchema).map(r => {
      Nation(
        r.getValue("n_nationkey").asInstanceOf[Int],
        r.getValue("n_name").asInstanceOf[String],
        r.getValue("n_regionkey").asInstanceOf[Int],
        r.getValue("n_comment").asInstanceOf[String])
    })
  }

  def regionRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "nation", scanQuery, tSchema).map(r => {
      Region(
        r.getValue("r_regionkey").asInstanceOf[Int],
        r.getValue("r_name").asInstanceOf[String],
        r.getValue("r_comment").asInstanceOf[String])
    })
  }

  def orderRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "nation", scanQuery, tSchema).map(r => {
      Order(
        r.getValue("o_orderkey").asInstanceOf[Int],
        r.getValue("o_custkey").asInstanceOf[Int],
        r.getValue("o_orderstatus").asInstanceOf[String],
        r.getValue("o_totalprice").asInstanceOf[Int],
        r.getValue("o_orderdate").asInstanceOf[String], //TODO change to long
        r.getValue("o_orderpriority").asInstanceOf[String],
        r.getValue("o_clerk").asInstanceOf[String],
        r.getValue("o_shippriority").asInstanceOf[Int],
        r.getValue("o_comment").asInstanceOf[String])
    })
  }

  def partRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "part", scanQuery, tSchema).map(r => {
      Part(
        r.getValue("p_partkey").asInstanceOf[Int],
        r.getValue("p_name").asInstanceOf[String],
        r.getValue("p_mfgr").asInstanceOf[String],
        r.getValue("p_brand").asInstanceOf[String],
        r.getValue("p_type").asInstanceOf[String],
        r.getValue("p_size").asInstanceOf[Int],
        r.getValue("p_container").asInstanceOf[String],
        r.getValue("p_retailprice").asInstanceOf[Double],
        r.getValue("p_comment").asInstanceOf[String])
    })
  }

  def partsuppRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "partsupp", scanQuery, tSchema).map(r => {
      Partsupp(
        r.getValue("ps_partkey").asInstanceOf[Int],
        r.getValue("ps_suppkey").asInstanceOf[Int],
        r.getValue("ps_availqty").asInstanceOf[Int],
        r.getValue("ps_supplycost").asInstanceOf[Double],
        r.getValue("ps_comment").asInstanceOf[String])
    })
  }

  def supplierRdd(scc: TSparkContext, scanQuery: ScanQuery, tSchema: TSchema) = {
    new TRDD[TRecord](scc, "supplier", scanQuery, tSchema).map(r => {
      Supplier(
        r.getValue("s_suppkey").asInstanceOf[Int],
        r.getValue("s_name").asInstanceOf[String],
        r.getValue("s_address").asInstanceOf[String],
        r.getValue("s_nationkey").asInstanceOf[Int],
        r.getValue("s_phone").asInstanceOf[String],
        r.getValue("s_acctbal").asInstanceOf[Double],
        r.getValue("s_comment").asInstanceOf[String])
    })
  }

  //val customer = sc.textFile(INPUT_DIR + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()
  //val lineitem = sc.textFile(INPUT_DIR + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()
  //  val nation = sc.textFile(INPUT_DIR + "/nation.tbl").map(_.split('|')).map(p => Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF()
  //  val region = sc.textFile(INPUT_DIR + "/region.tbl").map(_.split('|')).map(p => Region(p(0).trim.toInt, p(1).trim, p(1).trim)).toDF()
  //  val order = sc.textFile(INPUT_DIR + "/orders.tbl").map(_.split('|')).map(p => Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF()
  //  val part = sc.textFile(INPUT_DIR + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()
  //  val partsupp = sc.textFile(INPUT_DIR + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble, p(4).trim)).toDF()
  //  val supplier = sc.textFile(INPUT_DIR + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()
  var customer: DataFrame = null
  var lineitem: DataFrame = null
  var nation: DataFrame = null
  var region: DataFrame = null
  var order: DataFrame = null
  var part: DataFrame = null
  var partsupp: DataFrame = null
  var supplier: DataFrame = null

  def initRdds(masterUrl: String, appName: String, strMng: String, cmMng: String, chNum: Int, chSz: Int) = {
    val scc = new TSparkContext(masterUrl, appName, strMng, cmMng, chNum, chSz)
    customer = customerRdd(scc, new ScanQuery, TpchTSchema.customerSch).toDF
    lineitem = lineItemRdd(scc, new ScanQuery, TpchTSchema.lineItemSch).toDF
    nation = nationRdd(scc, new ScanQuery, TpchTSchema.nationSch).toDF
    region = regionRdd(scc, new ScanQuery, TpchTSchema.regionSch).toDF
    order = orderRdd(scc, new ScanQuery, TpchTSchema.orderSch).toDF
    part = partRdd(scc, new ScanQuery, TpchTSchema.partSch).toDF
    partsupp = partsuppRdd(scc, new ScanQuery, TpchTSchema.partSuppSch).toDF
    supplier = supplierRdd(scc, new ScanQuery, TpchTSchema.supplierSch).toDF
  }

  def executeQuery(masterUrl: String, appName: String, strMng: String, cmMng: String, chNum: Int, chSz: Int) = {
    initRdds(masterUrl, appName, strMng, cmMng, chNum, chSz)
    execute
  }

  /**
   * implemented in children classes and hold the actual query
   */
  def execute(): Unit

  def outputDF(df: DataFrame): Unit = {

    //TODO writing to a file might also be faster, as every worker will write their own part if possible
    if (OUTPUT_DIR == null || OUTPUT_DIR == "")
      df.collect().foreach(println)
    else
      df.write.mode("overwrite").json(OUTPUT_DIR + "/" + className + ".out") // json to avoid alias
  }
}

object TpchQuery {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Execute query reflectively
   */
  def executeQuery(queryNo: Int, st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    assert(queryNo >= 1 && queryNo <= 22, "Invalid query number")
    val m = Class.forName(f"ch.ethz.queries.tpch.Q${queryNo}%d").newInstance.asInstanceOf[ {def executeQuery(st: String, cm: String, cn: Int, cs: Int, mUrl: String)}]
    logger.info("[%s] Pre query execution".format(this.getClass.getName))
    val res = m.executeQuery(st, cm, cn, cs, mUrl)
    logger.info("[%s] Post query execution".format(this.getClass.getName))
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
        println("[TELL] <strMng> <commitMng> <partNum> <chunkSz> <masterUrl> <appName>")
        throw new RuntimeException("Invalid number of arguments")
      }
    }

    TClientFactory.setConf(st, cm, cs)
    TClientFactory.startTransaction()
    ChTSchema.init_schema(TClientFactory.mainTrx)
    TClientFactory.commitTrx()

    logger.warn("[%s] Query %d: %s".format(this.getClass.getName, qryNum, TClientFactory.toString))
    val excludeList = List(16, 20, 21)
    if (qryNum > 0) {
      executeQuery(qryNum, st, cm, cn, cs, masterUrl)
    } else {
      (1 to 22).map(i =>
        if (!excludeList.contains(i)) {
          logger.warn("Executing query " + i)
          executeQuery(i, st, cm, cn, cs, masterUrl)
        }
      )
    }
  }
}