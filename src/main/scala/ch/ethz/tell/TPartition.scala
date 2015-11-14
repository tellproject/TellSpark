package ch.ethz.tell

import org.apache.spark.Partition

/**
 * Contains basic information about TellStore partition
 */
class TPartition[T] (val index: Int, val scanIt: ScanIterator) extends Partition {

  override def toString = {
    val sb = new StringBuilder
    sb.append("{idx:").append(index).append(",")
    sb.append("length:").append(scanIt.length).append(",")
    sb.append("address:").append(scanIt.address)
    sb.append("}")
    sb.toString()
  }
}
