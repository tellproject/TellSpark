package ch.ethz

import ch.ethz.tell.ScanIterator
import org.apache.spark.Partition

/**
 * Contains basic information about TellStore partition
 */
class TellPartition[T] (val index: Int, val scanIt: ScanIterator) extends Partition {

  override def toString = {
    val sb = new StringBuilder
    sb.append("{idx:").append(index).append(",")
    sb.append("scanIt:").append(scanIt.toString)
    sb.append(",").append("}")
    sb.toString()
  }
}