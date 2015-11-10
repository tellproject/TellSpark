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
  var cnt: Int = 0

  def addField(fType: FieldType, fName: String, fNull: Boolean) = {
    fType match {
      case FieldType.NOTYPE | FieldType.NULLTYPE | FieldType.SMALLINT |
      FieldType.INT | FieldType.BIGINT | FieldType.FLOAT | FieldType.DOUBLE =>
        fixedSizeFields += fType
      case FieldType.TEXT | FieldType.BLOB =>
        varSizeFields += fType
    }
    fields += new TellField(cnt, fType, fName, fNull)
    cnt += 1
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

  override def toString() : String = {
    var sb = new StringBuilder
    fields.map(f => sb.append(f.toString()).append("\n"))
    sb.toString
  }

}
