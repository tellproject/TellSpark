package ch.ethz

import java.lang.reflect.Field

import sun.misc.Unsafe

/**
 * Object mocking the actual tell client
 */
object TellClient {

  // TODO we should get the number of partitions from tell
  // number of memory regions to be read
  val nPartitions: Int = 5
  val array = new Array[Long](nPartitions)
  // the transactions we need to pay attention to
  val trxId: Long = 0


  def getMemLocations() : Array[Long] = {
    if (array.length == 0) {
      val u: Unsafe = getUnsafe()
      val tester: NativeTester = new NativeTester
      (1 to nPartitions).map(n => {
        val memAddr: Long = tester.createStruct
        array(n) = memAddr
      })
    }
    array
  }

  //TODO this might not be required directly from here
  def rmMemLocations() = {
    val u: Unsafe = getUnsafe()
    val tester: NativeTester = new NativeTester
    array.map(tester.deleteStruct(_))
  }

  def getUnsafe(): Unsafe = {
    val singleoneInstanceField: Field = classOf[Unsafe].getDeclaredField("theUnsafe")
    singleoneInstanceField.setAccessible(true)
    singleoneInstanceField.get(null).asInstanceOf[Unsafe]
  }

}
