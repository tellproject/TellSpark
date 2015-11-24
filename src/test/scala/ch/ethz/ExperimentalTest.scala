package ch.ethz

import org.scalatest.FunSuite
import org.apache.spark.{SparkConf, SparkContext}

/**
 */
class ExperimentalTest extends FunSuite {
  val conf = new SparkConf().setMaster("local[*]").setAppName("test")
  // TODO create a specific sparkContext for holding the transaction manager
  val sc = new SparkContext(conf)

  test("Test") {
    val data = Array(1, 2, 3, 4, 5)
    val ddata = sc.parallelize(data)
//    readLine()
    ddata.count()
    println("====")
    assert(true)
  }
}
