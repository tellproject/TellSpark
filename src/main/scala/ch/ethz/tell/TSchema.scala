package ch.ethz.tell

import ch.ethz.tell.Field.FieldType

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Wrapper around the native schema class used in TellStore
 */
class TSchema(tellSchema: Schema) extends Serializable {

  {
    val fieldNames = tellSchema.getFieldNames
    for (fieldName <- fieldNames) {
      val field = tellSchema.getFieldByName(fieldName)
      fields += field
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

  var fixedSizeFields = ArrayBuffer[FieldType]()
  var varSizeFields = ArrayBuffer[FieldType]()
  var fields = ArrayBuffer[Field]()
  var strFields = HashMap[String, Field]()
  var cnt: Short = 0

  def getField(fieldName : String) :Field = {
    strFields(fieldName)
  }

  override def toString() : String = {
    val sb = new StringBuilder
    fields.map(f => sb.append(f.toString()).append("\n"))
    sb.toString
  }

}