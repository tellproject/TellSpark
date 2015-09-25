package ch.ethz

import org.apache.spark.Partition

/**
 * Contains basic information about TellStore partition
 */
class tellPartition[T] (val index: Int, val memAddr: Long) extends Partition {

  override def toString = {
    val sb = new StringBuilder
    sb.append("{idx:").append(index).append(",")
    sb.append("memAddres:").append(memAddr).append("}")
    sb.toString()
  }
}
