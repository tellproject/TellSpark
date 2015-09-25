package ch.ethz

import org.apache.spark.{SparkConf, SparkContext}

/**
 */
object Experimental {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
  val sc = new SparkContext(conf)
  def main(args : Array[String]) {
    val tellRdd = new TellRDD[Customer](sc, null)
    println("=============")
    tellRdd.map(println(_))
    tellRdd.collect()
  }
}
