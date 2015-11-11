package ch.ethz.tell

import org.apache.spark.Partition

/**
 * Contains basic information about TellStore partition
 */
class TPartition[T] (val index: Int, val scanIt: ScanIterator) extends Partition {

  override def toString = {
    val sb = new StringBuilder
    sb.append("{idx:").append(index).append(",")
    sb.append("scanIt:").append(scanIt.toString)
    sb.append(",").append("}")
    sb.toString()
  }
}
