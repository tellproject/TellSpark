package ch.ethz

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by marenato on 24.11.15.
 */
object Debugger {

  def main (args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val ddata = sc.parallelize(Array(1, 2, 3, 4, 5))
    readLine()
    ddata.count()
    println("====")

  }
}
