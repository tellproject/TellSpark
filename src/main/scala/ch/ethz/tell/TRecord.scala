package ch.ethz.tell

/**
 * Generic TellRecord wrapper
 */
class TRecord (var fieldSchema: TSchema, var values : Array[Any])
  extends Serializable {

  def this(fSchema: TSchema, valSz: Int)= {
    this(fSchema, new Array[Any](valSz))
  }

  /**
    * get tuple (field description and value) by field index
    */
  def getField(idx : Short) = {
    (fieldSchema.getField(idx), values(idx))
  }

  /**
    * get tuple (field description and value) by name
    */
  def getField(fieldName : String) = {
    val field = fieldSchema.getField(fieldName)
    (field, values(field.index))
  }

  /**
    * get only value of a field by field index
    */
  def getValue(idx : Short) = {
    val tuple = getField(idx)
    tuple._2
  }

  /**
    * get only value of a field by name
    */
  def getValue(fieldName : String) = {
    val tuple = getField(fieldName)
    tuple._2
  }

  /**
    * sets the value of a field
    */
  def setField(idx: Short, value: Any) = {
    values(idx) = value
  }

  override def toString():String = {
    val sb = new StringBuilder
    sb.append("{")
    (0 to fieldSchema.getSize()-1).map(entry => {
      val tuple = getField(entry.asInstanceOf[Short])
      sb.append(tuple._1.fieldName).append(":")
      sb.append(tuple._2)
    })
    sb.append("}")
    sb.toString()
  }
}
