package ch.ethz.tell

/**
 * Created by marenato on 10.11.15.
 */
class TRecord (var fieldSchema: TSchema, var values : Array[Any])
  extends Serializable {

  def this(fSchema: TSchema, valSz: Int)= {
    this(fSchema, new Array[Any](valSz))
  }

  //TODO:
//  def getComplete() = {
//    (fieldSchema.fields., values)
//  }

  def getField(idx : Short) = {
    (fieldSchema.fields(idx), values(idx))
  }

  def getField(fieldName : String) = {
    fieldSchema.strFields(fieldName)
  }

  def setField(idx: Short, value: Any) = {
    values(idx) = value
  }

  def getValue(fieldName : String) = {
    val idx = fieldSchema.getField(fieldName).index
    values(idx)
  }
  override def toString():String = {
    val sb = new StringBuilder
    sb.append("{")
    fieldSchema.strFields.map(entry => {
      sb.append(entry._1).append(":")
      sb.append(values(fieldSchema.getField(entry._1).index))
    })
    sb.append("}")
    sb.toString()
  }
}
