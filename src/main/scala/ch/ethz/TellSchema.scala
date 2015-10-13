package ch.ethz

import scala.collection.mutable.ArrayBuffer
import ch.ethz.tell.Schema.FieldType

/**
 * Wrapper around the native schema class used in TellStore
 */
class TellSchema() extends Serializable {

  var fixedSizeFields = ArrayBuffer[FieldType]()
  var varSizeFields = ArrayBuffer[FieldType]()
  var fields = ArrayBuffer[TellField]()
  var length: Int = 0

  def addField(fType: FieldType, fName: String, fNull: Boolean) = {
    fType match {
      case FieldType.NOTYPE =>
      case FieldType.NULLTYPE =>
      case FieldType.SMALLINT =>
      case FieldType.INT =>
      case FieldType.BIGINT =>
      case FieldType.FLOAT =>
      case FieldType.DOUBLE =>
        fixedSizeFields += fType
      case FieldType.TEXT =>
      case FieldType.BLOB =>
        varSizeFields += fType
    }
    fields += new TellField(length, fType, fName, fNull)
  }

  class TellField(var index: Int, var fieldType: FieldType, var fieldName: String, var nullable: Boolean)
    extends Serializable {

    override def toString () : String = {
      var sb = new StringBuilder
      sb.append("{index:").append(index).append(", ")
      sb.append("type:").append(fieldType).append(", ")
      sb.append("name:").append(fieldName).append(", ")
      sb.append("null:").append(nullable).append("}")
      sb.toString()
    }
  }

}
