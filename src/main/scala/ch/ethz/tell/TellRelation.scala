package ch.ethz.tell

import ch.ethz.tell.Field.FieldType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.rdd.RDD

case class TellPartition(partition: Int) extends Partition {
  override def index: Int = partition
}

class DefaultSource extends RelationProvider with DataSourceRegister {
  override def shortName(): String = "tell"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val table = parameters.getOrElse("table", sys.error("Option 'table' not specified"))

    val useSmallMemory = false //parameters.getOrElse("useSmallMemory", "false").toBoolean

    val numPartitions = parameters.getOrElse("numPartitions", sys.error("Option 'numPartitions' not specified")).toInt
    if (numPartitions < 1) {
      sys.error("Number of partitions must be greater than 0")
    }
    val partitions = Array.range(0, numPartitions).map(i => TellPartition(i))

    TellRelation(table, partitions.asInstanceOf[Array[Partition]], useSmallMemory)(sqlContext)
  }
}

object TellRelation {
  def getCatalystType(fieldType: FieldType): DataType = {
    fieldType match {
      case FieldType.SMALLINT => ShortType
      case FieldType.INT => IntegerType
      case FieldType.BIGINT => LongType
      case FieldType.FLOAT => FloatType
      case FieldType.DOUBLE => DoubleType
      case FieldType.BLOB => BinaryType
      case FieldType.TEXT => StringType
      case _ => sys.error(s"Unsupported type ${fieldType}")
    }
  }
}

case class TellRelation(
    table: String,
    partitions: Array[Partition],
    useSmallMemory: Boolean
  )(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan {

  @transient
  val context = sqlContext.asInstanceOf[TellContext]

  override val needConversion: Boolean = false

  override val schema: StructType = {
    val transaction = context.transaction
    val srcSchema = transaction.schemaForTable(table)
    val names = srcSchema.getFieldNames
    val fields = new Array[StructField](names.length)
    var i = 0
    for (name <- names) {
      val field = srcSchema.getFieldByName(name)
      val columnType = TellRelation.getCatalystType(field.fieldType)
      fields(i) = StructField(name, columnType, !field.notNull)
      i = i + 1
    }
    StructType(fields)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    new TellRDD(context, context.transactionId, table, requiredColumns, filters, partitions, useSmallMemory)
      .asInstanceOf[RDD[Row]]
  }
}
