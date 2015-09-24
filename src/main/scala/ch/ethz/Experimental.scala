package ch.ethz

import org.apache.spark.{SparkConf, SparkContext}
import ch.ethz.Customer;
import ch.ethz.Main;

/**
 */
object Experimental {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
  val sc = new SparkContext(conf)
  def main(args : Array[String]) {
    val c = Main.getSingleCostumer
    println(c.toString)
    sc.parallelize(1 to 10).collect().foreach(println)
  }
}
