package ch.ethz.tell

import org.apache.spark.Partition

/**
 * Contains basic information about TellStore partition
 */
class TPartition[T] (val index: Int, val scanQry: ScanQuery, val tableName: String) extends Partition {

  override def toString = {
    val sb = new StringBuilder
    sb.append("{idx:").append(index).append(",")
    sb.append("query:").append(scanQry.toString).append(",")
    sb.append("tableName:").append(tableName).append(",")
    sb.append("}")
    sb.toString()
  }
}
