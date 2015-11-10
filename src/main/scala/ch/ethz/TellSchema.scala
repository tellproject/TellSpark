package ch.ethz

import scala.collection.mutable.ArrayBuffer
import ch.ethz.tell.Schema.FieldType
import collection.mutable.HashMap

/**
 * Wrapper around the native schema class used in TellStore
 */
class TellSchema() extends Serializable {

  var fixedSizeFields = ArrayBuffer[FieldType]()
  var varSizeFields = ArrayBuffer[FieldType]()
  var fields = ArrayBuffer[TellField]()
  var strFields = HashMap[String, TellField]()
  var cnt: Int = 0

  def addField(fType: FieldType, fName: String, fNull: Boolean) = {
    fType match {
      case FieldType.NOTYPE | FieldType.NULLTYPE | FieldType.SMALLINT |
      FieldType.INT | FieldType.BIGINT | FieldType.FLOAT | FieldType.DOUBLE =>
        fixedSizeFields += fType
      case FieldType.TEXT | FieldType.BLOB =>
        varSizeFields += fType
    }
    val tf = new TellField(cnt, fType, fName, fNull)
    fields += tf
    strFields.put(fName, tf)
    cnt += 1
  }

  def getField(fieldName : String) :TellField = {
    strFields(fieldName)
  }
  override def toString() : String = {
    var sb = new StringBuilder
    fields.map(f => sb.append(f.toString()).append("\n"))
    sb.toString
  }
}

class TellField(var index: Int, var fieldType: FieldType, var fieldName: String, var nullable: Boolean)
  extends Serializable {

  override def toString(): String = {
    var sb = new StringBuilder
    sb.append("{index:").append(index).append(", ")
    sb.append("type:").append(fieldType).append(", ")
    sb.append("name:").append(fieldName).append(", ")
    sb.append("null:").append(nullable).append("}")
    sb.toString()
  }
}