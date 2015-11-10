package ch.ethz


import scala.collection.mutable.ArrayBuffer

/**
 * Created by marenato on 10.11.15.
 */
class TellRecord (val fieldSchema: TellSchema, val values : ArrayBuffer[Any])
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

  def getValue(fieldName : String) = {
    val tellField = fieldSchema.strFields(fieldName)
    val idx = fieldSchema.fields.indexOf(tellField)
    values(idx)
  }
}