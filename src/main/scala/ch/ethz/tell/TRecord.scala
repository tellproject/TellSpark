package ch.ethz.tell

import scala.collection.mutable.ArrayBuffer

/**
 * Created by marenato on 10.11.15.
 */
class TRecord (val fieldSchema: TSchema, val values : ArrayBuffer[Any])
  extends Serializable {


  def getComplete() = {
    (fieldSchema.fields, values)
  }
  def getField(idx : Int) = {
    (fieldSchema.fields(idx), values(idx))
  }

  def getField(fieldName : String) = {
    fieldSchema.strFields(fieldName)
  }

  def setField(idx: Int, value: Any) = {
    values(idx) = value
  }

  def getValue(fieldName : String) = {
    val tellField = fieldSchema.strFields(fieldName)
    val idx = fieldSchema.fields.indexOf(tellField)
    values(idx)
  }
}