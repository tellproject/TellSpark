package ch.ethz.queries

import ch.ethz.TellSchema
import ch.ethz.tell.Schema

/**
 * Created by marenato on 10.11.15.
 */
object CHSchema {

  /**
   * ORDERLINE\" (\n"
			"	\"OL_O_ID\" INTEGER CS_INT,\n"
			"	\"OL_D_ID\" TINYINT CS_INT,\n"
			"	\"OL_W_ID\" INTEGER CS_INT,\n"
			"	\"OL_NUMBER\" TINYINT CS_INT,\n"
			"	\"OL_I_ID\" INTEGER CS_INT,\n"
			"	\"OL_SUPPLY_W_ID\" INTEGER CS_INT,\n"
			"	\"OL_DELIVERY_D\" SECONDDATE CS_SECONDDATE,\n"
			"	\"OL_QUANTITY\" SMALLINT CS_INT,\n"
			"	\"OL_AMOUNT\" DECIMAL(6,2) CS_FIXED,\n"
			"	\"OL_DIST_INFO\" CHAR(24) CS_FIXEDSTRING,\n
   */
  val orderLineSch: TellSchema = new TellSchema()
  orderLineSch.addField(Schema.FieldType.INT, "OL_O_ID", false)
  orderLineSch.addField(Schema.FieldType.SMALLINT, "OL_D_ID", false)
  orderLineSch.addField(Schema.FieldType.INT, "OL_W_ID", false)
  orderLineSch.addField(Schema.FieldType.SMALLINT, "OL_NUMBER", false)
  orderLineSch.addField(Schema.FieldType.INT, "OL_I_ID", true)
  orderLineSch.addField(Schema.FieldType.INT, "OL_SUPPLY_W_ID", true)
  orderLineSch.addField(Schema.FieldType.BIGINT, "OL_DELIVERY_D", true)
  orderLineSch.addField(Schema.FieldType.SMALLINT, "OL_QUANTITY", true)
  orderLineSch.addField(Schema.FieldType.DOUBLE, "OL_AMOUNT", true)
  orderLineSch.addField(Schema.FieldType.TEXT, "OL_DIST_INFO", true)

}
