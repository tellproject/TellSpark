package ch.ethz.tell

import ch.ethz.tell.Field.FieldType

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Wrapper around the native schema class used in TellStore
 */
class TSchema extends Serializable {

  var fixedSizeFields = ArrayBuffer[FieldType]()
  var varSizeFields = ArrayBuffer[FieldType]()
  private var fields = HashMap[Short, Field]()
  private var strFields = HashMap[String, Field]()
  private var headerLength = 0L

  def this(tellSchema: Schema) {
    this()
    headerLength = tellSchema.getHeaderLength
    val fieldNames = tellSchema.getFieldNames
    for (fieldName <- fieldNames) {
      val field = tellSchema.getFieldByName(fieldName)
      fields.put(field.index, field)
      strFields.put(field.fieldName, field)
      val fType = field.fieldType
      fType match {
        case FieldType.NOTYPE | FieldType.NULLTYPE | FieldType.SMALLINT |
          FieldType.INT | FieldType.BIGINT | FieldType.FLOAT | FieldType.DOUBLE =>
          fixedSizeFields += fType
        case FieldType.TEXT | FieldType.BLOB =>
          varSizeFields += fType
      }
    }
  }

  def getField(fieldName : String) :Field = {
    strFields(fieldName)
  }

  def getField(fieldIndex : Short) :Field = {
    fields(fieldIndex)
  }

  def getSize() : Short = {
    fields.size.asInstanceOf[Short]
  }

  def getHeaderLength() : Long = {
    headerLength
  }

  def createProjection(fieldIndex: Short) : Projection = {
    val field = getField(fieldIndex)
    new Projection(field.index, field.fieldName, field.fieldType, field.nullable)
  }

  def createProjection(fieldName: String) : Projection = {
    val field = getField(fieldName)
    new Projection(field.index, field.fieldName, field.fieldType, field.nullable)
  }

  def printFixedSize():String = {
    val sb = new StringBuilder
    sb.append("{")
    fixedSizeFields.map(sb.append(_).append("-"))
    sb.toString()
  }

  def printVarFixedSize():String = {
    val sb = new StringBuilder
    sb.append("{")
    varSizeFields.map(sb.append(_).append("-"))
    sb.toString()
  }

  override def toString() : String = {
    val sb = new StringBuilder
    fields.map(f => sb.append(f.toString()).append("\nFixed:").append(printFixedSize()).append("\nVarSize:").append(varSizeFields))
    sb.toString
  }

}