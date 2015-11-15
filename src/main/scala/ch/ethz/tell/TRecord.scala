package ch.ethz.tell

import scala.collection.mutable.ArrayBuffer

/**
 * Created by marenato on 10.11.15.
 */
class TRecord (var fieldSchema: TSchema, var values : Array[Any])
  extends Serializable {

  def this(fSchema: TSchema, valSz: Int)= {
    this(fSchema, new Array[Any](valSz))
  }

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
    if (values.size == 0) println("cacacacacacacaca")
    values(idx) = value
  }

  def getValue(fieldName : String) = {
    val tellField = fieldSchema.strFields(fieldName)
    val idx = fieldSchema.fields.indexOf(tellField)
    values(idx)
  }
}
