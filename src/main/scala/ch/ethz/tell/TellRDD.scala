package ch.ethz.tell

import ch.ethz.tell.Field.FieldType
import ch.ethz.tell.ScanQuery.CmpType
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.sources.GreaterThan
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.sources.IsNotNull
import org.apache.spark.sql.sources.IsNull
import org.apache.spark.sql.sources.LessThan
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.unsafe.types.UTF8String

case class FieldMetaData(
    fieldType: FieldType,
    idx: Int,
    notNull: Boolean)

object TellRDD extends Logging {
  def getPredicateType(value: Any): PredicateType = value match {
    case data: Short => PredicateType.create(data)
    case data: Int => PredicateType.create(data)
    case data: Long => PredicateType.create(data)
    case data: Float => PredicateType.create(data)
    case data: Double => PredicateType.create(data)
    case data: String => PredicateType.create(data)
    case _ => null
  }
}

/**
 * RDD class for connecting to TellStore
 */
class TellRDD(
    context: TellContext,
    transactionId: Long,
    table: String,
    requiredColumns: Array[String],
    filters: Array[Filter],
    partitions: Array[Partition],
    useSmallMemory: Boolean)
  extends RDD[InternalRow](context.sparkContext, Nil) with Logging {

  override def getPartitions: Array[Partition] = partitions

  // compiles a (possibly nested) filter
  def compileFilter (filter:Filter, clause:CNFClause, srcSchema:Schema): Unit = {
    filter match {
      case And(left, right) => throw new RuntimeException("AND should never appear within a filter as each" +
        " conjunct is supposed to appear in its proper filter! --> check API of interfaces.scala:PrunedFilteredScan")
      case Or(left, right) =>
        compileFilter(left, clause, srcSchema)
        compileFilter(right, clause, srcSchema)
      case EqualTo(attr, value) =>
        clause.addPredicate(CmpType.EQUAL, srcSchema.idOf(attr), TellRDD.getPredicateType(value))
      case LessThan(attr, value) =>
        clause.addPredicate(CmpType.LESS, srcSchema.idOf(attr), TellRDD.getPredicateType(value))
      case GreaterThan(attr, value) =>
        clause.addPredicate(CmpType.GREATER, srcSchema.idOf(attr), TellRDD.getPredicateType(value))
      case LessThanOrEqual(attr, value) =>
        clause.addPredicate(CmpType.LESS_EQUAL, srcSchema.idOf(attr), TellRDD.getPredicateType(value))
      case GreaterThanOrEqual(attr, value) =>
        clause.addPredicate(CmpType.GREATER_EQUAL, srcSchema.idOf(attr), TellRDD.getPredicateType(value))
      case IsNull(attr) => clause.addPredicate(CmpType.IS_NULL, srcSchema.idOf(attr), null)
      case IsNotNull(attr) => clause.addPredicate(CmpType.IS_NOT_NULL, srcSchema.idOf(attr), null)
      case StringStartsWith(attr, value) =>
        clause.addPredicate(CmpType.PREFIX_LIKE, srcSchema.idOf(attr), TellRDD.getPredicateType(value))
      case StringEndsWith(attr, value) =>
        clause.addPredicate(CmpType.POSTFIX_LIKE, srcSchema.idOf(attr), TellRDD.getPredicateType(value))
    }
  }

  override def compute(split: Partition, task: TaskContext): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      logInfo(s"Iterating through a TellRDD split [transaction = ${transactionId}, table = ${table}, " +
        s"split = ${split.index} / ${partitions.length}]")

      val connection = context.connection
      val transaction = connection.startTransaction(transactionId)

      val srcSchema = transaction.schemaForTable(table)

      val scanQuery = {
        val numPartitions = {
          if (partitions.length > 1) {
            partitions.length
          } else {
            0
          }
        }
        val srcSchema = transaction.schemaForTable(table)

        val query = new ScanQuery(table, split.index, numPartitions)
        for (name <- requiredColumns) {
          val field = srcSchema.getFieldByName(name)
          query.addProjection(field.index, field.fieldName, field.fieldType, field.notNull)
        }
        for (filter <- filters) {
          val clause = new CNFClause
          compileFilter(filter, clause, srcSchema)
          if (clause.numPredicates() > 0) {
            query.addSelection(clause)
          }
        }
        query
      }

      var schema:Schema = null
      try {
        schema = scanQuery.getResultSchema
      } catch {
        case e : RuntimeException => {
          // if there is no result schema (because there is no projection or aggregation), then use the source schema
          schema = srcSchema
        }
      }

      val headerLength = schema.getHeaderLength

      val fieldMeta: Array[FieldMetaData] = {
        val metadata = new Array[FieldMetaData](requiredColumns.length)
        var i = 0
        for (name <- requiredColumns) {
          val field = schema.getFieldByName(name)
          metadata(field.index) = FieldMetaData(field.fieldType, i, field.notNull)
          i = i + 1
        }
        metadata
      }

      val mutableRow: SpecificMutableRow = {
        val values = requiredColumns.map(name => {
          val field = schema.getFieldByName(name)
          TellRelation.getCatalystType(field.fieldType)
        })
        new SpecificMutableRow(values)
      }

      val iter: ScanIterator = {
        val scanMemoryManager = {
          if (useSmallMemory) {
            connection.memoryManagerSmall
          } else {
            connection.memoryManagerBig
          }
        }
        transaction.scan(scanMemoryManager, scanQuery)
      }

      var finished = false
      var chunkPos = 0L
      var chunkEnd = 0L

      val unsafe: sun.misc.Unsafe = Unsafe.getUnsafe()

      override def hasNext: Boolean = {
        if (!finished && chunkPos == chunkEnd) {
          val hasChunk = iter.next()
          if (hasChunk) {
            chunkPos = iter.address()
            chunkEnd = chunkPos + iter.length()
          } else {
            transaction.commit()
            finished = true
          }
        }
        !finished
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException("End of stream")
        }

        // Skip key
        chunkPos += 8

        // Skip header
        val record = chunkPos
        chunkPos += headerLength

        // Align for fields
        if (chunkPos % 8 != 0) chunkPos += 8 - chunkPos % 8

        var hasVariable = false
        // The size of the variable heap
        // Initialized to 4 to skip the additional offset field at the end
        var variableLength = 4

        var nullIdx = 0
        for (field: FieldMetaData <- fieldMeta) {
          field.fieldType match {
            case FieldType.SMALLINT =>
              mutableRow.setShort(field.idx, unsafe.getShort(chunkPos))
              chunkPos += 2

            case FieldType.INT =>
              mutableRow.setInt(field.idx, unsafe.getInt(chunkPos))
              chunkPos += 4

            case FieldType.BIGINT =>
              mutableRow.setLong(field.idx, unsafe.getLong(chunkPos))
              chunkPos += 8

            case FieldType.FLOAT =>
              mutableRow.setFloat(field.idx, unsafe.getFloat(chunkPos))
              chunkPos += 4

            case FieldType.DOUBLE =>
              mutableRow.setDouble(field.idx, unsafe.getDouble(chunkPos))
              chunkPos += 8

            case FieldType.TEXT | FieldType.BLOB =>
              if (!hasVariable) {
                hasVariable = true

                // Align to 4 for the first variable size offset
                if (chunkPos % 4 != 0) chunkPos += 4 - chunkPos % 4
              }

              val offset = unsafe.getInt(chunkPos)
              val length = unsafe.getInt(chunkPos + 4) - offset
              variableLength += length

              val pos = record + offset
              val value = new Array[Byte](length)
              for (i: Int <- 0 to length - 1) {
                value(i) = unsafe.getByte(pos + i)
              }

              val data: Any = {
                if (field.fieldType == FieldType.TEXT) {
                  UTF8String.fromBytes(value)
                } else {
                  value
                }
              }
              mutableRow.update(field.idx, data)
              chunkPos += 4

            case _ => sys.error(s"Unsupported type ${field.fieldType}")
          }
          if (!field.notNull) {
            if (unsafe.getByte(record + nullIdx) != 0) {
              mutableRow.setNullAt(field.idx)
            }
            nullIdx = nullIdx + 1
          }
        }
        // Skip the heap if the record has any variable size fields
        if (hasVariable) {
          chunkPos += variableLength
        }

        // Align to next record
        if (chunkPos % 8 != 0) chunkPos += 8 - chunkPos % 8

        mutableRow
      }
    }
  }
}
