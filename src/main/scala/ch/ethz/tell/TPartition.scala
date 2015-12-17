package ch.ethz.tell

import ch.ethz.TScanQuery
import org.apache.spark.Partition

/**
 * Contains basic information about TellStore partition
 */
class TPartition[T] (val index: Int, val scanQry: TScanQuery) extends Partition {

  override def toString = {
    val sb = new StringBuilder
    sb.append("{idx:").append(index).append(",")
    sb.append("query:").append(scanQry.toString).append(",")
    sb.append("tableName:").append(scanQry.getTableName).append(",")
    sb.append("}")
    sb.toString()
  }
}
