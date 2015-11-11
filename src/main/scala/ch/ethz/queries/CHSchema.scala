package ch.ethz.queries

import ch.ethz.tell.{TellSchema, Schema}

/**
 * Created by marenato on 10.11.15.
 */
object CHSchema {
  /**
   * \"WAREHOUSE\" (\n"
			"	\"W_ID\" INTEGER CS_INT,\n"
			"	\"W_NAME\" CHAR(10) CS_FIXEDSTRING,\n"
			"	\"W_STREET_1\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"W_STREET_2\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"W_CITY\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"W_STATE\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"W_ZIP\" CHAR(9) CS_FIXEDSTRING,\n"
			"	\"W_TAX\" DECIMAL(4,4) CS_FIXED,\n"
			"	\"W_YTD\" DECIMAL(12,2) CS_FIXED,\n"
   */
  //TODO order by size
  val warehouseSch: TellSchema = new TellSchema()
  warehouseSch.addField(Schema.FieldType.INT, "W_ID", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "W_NAME", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "W_STREET_1", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "W_STREET_2", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "W_CITY", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "W_STATE", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "W_ZIP", false)
  warehouseSch.addField(Schema.FieldType.DOUBLE, "W_TAX", false)
  warehouseSch.addField(Schema.FieldType.DOUBLE, "W_YTD", false)

  /**
   * \"DISTRICT\" (\n"
			"	\"D_ID\" TINYINT CS_INT,\n"
			"	\"D_W_ID\" INTEGER CS_INT,\n"
			"	\"D_NAME\" CHAR(10) CS_FIXEDSTRING,\n"
			"	\"D_STREET_1\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"D_STREET_2\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"D_CITY\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"D_STATE\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"D_ZIP\" CHAR(9) CS_FIXEDSTRING,\n"
			"	\"D_TAX\" DECIMAL(4,4) CS_FIXED,\n"
			"	\"D_YTD\" DECIMAL(12,2) CS_FIXED,\n"
			"	\"D_NEXT_O_ID\" INTEGER CS_INT,\n"
   */
  val districtSch: TellSchema = new TellSchema()
  districtSch.addField(Schema.FieldType.SMALLINT, "D_ID", false)
  districtSch.addField(Schema.FieldType.INT, "D_W_ID", false)
  districtSch.addField(Schema.FieldType.TEXT, "D_NAME", false)
  districtSch.addField(Schema.FieldType.TEXT, "D_STREET_1", false)
  districtSch.addField(Schema.FieldType.TEXT, "D_STREET_2", false)
  districtSch.addField(Schema.FieldType.TEXT, "D_CITY", false)
  districtSch.addField(Schema.FieldType.TEXT, "D_STATE", false)
  districtSch.addField(Schema.FieldType.TEXT, "D_ZIP", false)
  districtSch.addField(Schema.FieldType.DOUBLE, "D_TAX", false)
  districtSch.addField(Schema.FieldType.DOUBLE, "D_YTD", false)
  districtSch.addField(Schema.FieldType.INT, "D_NEXT_O_ID", false)

  /**
   * "CUSTOMER\" (\n"
			"	\"C_ID\" SMALLINT CS_INT,\n"
			"	\"C_D_ID\" TINYINT CS_INT,\n"
			"	\"C_W_ID\" INTEGER CS_INT,\n"
			"	\"C_FIRST\" CHAR(16) CS_FIXEDSTRING,\n"
			"	\"C_MIDDLE\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"C_LAST\" CHAR(16) CS_FIXEDSTRING,\n"
			"	\"C_STREET_1\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"C_STREET_2\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"C_CITY\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"C_STATE\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"C_ZIP\" CHAR(9) CS_FIXEDSTRING,\n"
			"	\"C_PHONE\" CHAR(16) CS_FIXEDSTRING,\n"
			"	\"C_SINCE\" SECONDDATE CS_SECONDDATE,\n"
			"	\"C_CREDIT\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"C_CREDIT_LIM\" DECIMAL(12,2) CS_FIXED,\n"
			"	\"C_DISCOUNT\" DECIMAL(4,4) CS_FIXED,\n"
			"	\"C_BALANCE\" DECIMAL(12,2) CS_FIXED,\n"
			"	\"C_YTD_PAYMENT\" DECIMAL(12,2) CS_FIXED,\n"
			"	\"C_PAYMENT_CNT\" SMALLINT CS_INT,\n"
			"	\"C_DELIVERY_CNT\" SMALLINT CS_INT,\n"
			"	\"C_DATA\" CHAR(500) CS_FIXEDSTRING,\n"
			"	\"C_N_NATIONKEY\" INTEGER CS_INT,\n"
   */
  val customerSch: TellSchema = new TellSchema()
  customerSch.addField(Schema.FieldType.SMALLINT, "C_ID", false)
  customerSch.addField(Schema.FieldType.SMALLINT, "C_D_ID", false)
  customerSch.addField(Schema.FieldType.INT, "C_W_ID", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_FIRST", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_MIDDLE", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_LAST", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_STREET_1", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_STREET_2", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_CITY", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_STATE", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_ZIP", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_PHONE", false)
  customerSch.addField(Schema.FieldType.BIGINT, "C_SINCE", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_CREDIT", false)
  customerSch.addField(Schema.FieldType.DOUBLE, "C_CREDIT_LIM", false)
  customerSch.addField(Schema.FieldType.DOUBLE, "C_DISCOUNT", false)
  customerSch.addField(Schema.FieldType.DOUBLE, "C_BALANCE", false)
  customerSch.addField(Schema.FieldType.DOUBLE, "C_YTD_PAYMENT", false)
  customerSch.addField(Schema.FieldType.SMALLINT, "C_PAYMENT_CNT", false)
  customerSch.addField(Schema.FieldType.SMALLINT, "C_DELIVERY_CNT", false)
  customerSch.addField(Schema.FieldType.TEXT, "C_DATA", false)
  customerSch.addField(Schema.FieldType.INT, "C_N_NATIONKEY", false)

  /**
   * "HISTORY\" (\n"
			"	\"H_C_ID\" SMALLINT CS_INT,\n"
			"	\"H_C_D_ID\" TINYINT CS_INT,\n"
			"	\"H_C_W_ID\" INTEGER CS_INT,\n"
			"	\"H_D_ID\" TINYINT CS_INT,\n"
			"	\"H_W_ID\" INTEGER CS_INT,\n"
			"	\"H_DATE\" SECONDDATE CS_SECONDDATE,\n"
			"	\"H_AMOUNT\" DECIMAL(6,2) CS_FIXED,\n"
			"	\"H_DATA\" CHAR(24) CS_FIXEDSTRING\n"
   */
  val historySch: TellSchema = new TellSchema()
  historySch.addField(Schema.FieldType.SMALLINT, "H_C_ID", false)
  historySch.addField(Schema.FieldType.SMALLINT, "H_C_D_ID", false)
  historySch.addField(Schema.FieldType.INT, "H_C_W_ID", false)
  historySch.addField(Schema.FieldType.SMALLINT, "H_D_ID", false)
  historySch.addField(Schema.FieldType.INT, "H_W_ID", false)
  historySch.addField(Schema.FieldType.BIGINT, "H_DATE", false)
  historySch.addField(Schema.FieldType.DOUBLE, "H_AMOUNT", false)
  historySch.addField(Schema.FieldType.TEXT, "H_DATA", false)

  /**
   * "NEWORDER\" (\n"
			"	\"NO_O_ID\" INTEGER CS_INT,\n"
			"	\"NO_D_ID\" TINYINT CS_INT,\n"
			"	\"NO_W_ID\" INTEGER CS_INT,\n"
   */
  val newOrderSch: TellSchema = new TellSchema()
  newOrderSch.addField(Schema.FieldType.INT, "NO_O_ID", false)
  newOrderSch.addField(Schema.FieldType.SMALLINT, "NO_D_ID", false)
  newOrderSch.addField(Schema.FieldType.INT, "NO_W_ID", false)

  /**
   * "ORDER\" (\n"
			"	\"O_ID\" INTEGER CS_INT,\n"
			"	\"O_D_ID\" TINYINT CS_INT,\n"
			"	\"O_W_ID\" INTEGER CS_INT,\n"
			"	\"O_C_ID\" SMALLINT CS_INT,\n"
			"	\"O_ENTRY_D\" SECONDDATE CS_SECONDDATE,\n"
			"	\"O_CARRIER_ID\" TINYINT CS_INT,\n"
			"	\"O_OL_CNT\" TINYINT CS_INT,\n"
			"	\"O_ALL_LOCAL\" TINYINT CS_INT,\n"
   */
  val orderSch: TellSchema = new TellSchema()
  orderSch.addField(Schema.FieldType.INT, "O_ID", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "O_D_ID", false)
  orderSch.addField(Schema.FieldType.INT, "O_W_ID", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "O_C_ID", false)
  orderSch.addField(Schema.FieldType.BIGINT, "O_ENTRY_D", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "O_CARRIER_ID", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "O_OL_CNT", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "O_ALL_LOCAL", false)

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

  /**
   *\"NATION\" (\n"
			"	\"N_NATIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"N_NAME\" CHAR(25) CS_FIXEDSTRING NOT NULL,\n"
			"	\"N_REGIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"N_COMMENT\" CHAR(152) CS_FIXEDSTRING NOT NULL,\n"
   */
  val nationSch: TellSchema = new TellSchema()
  nationSch.addField(Schema.FieldType.SMALLINT, "N_NATIONKEY", false)
  nationSch.addField(Schema.FieldType.TEXT, "N_NAME", false)
  nationSch.addField(Schema.FieldType.SMALLINT, "N_REGIONKEY", false)
  nationSch.addField(Schema.FieldType.TEXT, "N_COMMENT", false)

  /**
   * \"REGION\" (\n"
			"	\"R_REGIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"R_NAME\" CHAR(55) CS_FIXEDSTRING NOT NULL,\n"
			"	\"R_COMMENT\" CHAR(152) CS_FIXEDSTRING NOT NULL,\n"
   */
  val regionSch: TellSchema = new TellSchema()
  regionSch.addField(Schema.FieldType.SMALLINT, "R_REGIONKEY", false)
  regionSch.addField(Schema.FieldType.TEXT, "R_NAME", false)
  regionSch.addField(Schema.FieldType.TEXT, "R_COMMENT", false)

  /** "SUPPLIER\" (\n"
			"	\"SU_SUPPKEY\" SMALLINT CS_INT NOT NULL,\n"
			"	\"SU_NAME\" CHAR(25) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_ADDRESS\" CHAR(40) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_NATIONKEY\"TINYINT CS_INT NOT NULL,\n"
			"	\"SU_PHONE\" CHAR(15) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_ACCTBAL\" DECIMAL(12,2) CS_FIXED NOT NULL,\n"
			"	\"SU_COMMENT\" CHAR(101) CS_FIXEDSTRING NOT NULL,\n"
   */
  val supplierSch: TellSchema = new TellSchema()
  supplierSch.addField(Schema.FieldType.SMALLINT, "SU_SUPPKEY", false)
  supplierSch.addField(Schema.FieldType.TEXT, "SU_NAME", false)
  supplierSch.addField(Schema.FieldType.TEXT, "SU_ADDRESS", false)
  supplierSch.addField(Schema.FieldType.SMALLINT, "SU_NATIONKEY", false)
  supplierSch.addField(Schema.FieldType.TEXT, "SU_PHONE", false)
  supplierSch.addField(Schema.FieldType.DOUBLE, "SU_ACCTBAL", false)
  supplierSch.addField(Schema.FieldType.TEXT, "SU_COMMENT", false)

  /**
   * \"STOCK\" (\n"
			"	\"S_I_ID\" INTEGER CS_INT,\n"
			"	\"S_W_ID\" INTEGER CS_INT,\n"
			"	\"S_QUANTITY\" SMALLINT CS_INT,\n"
			"	\"S_DIST_01\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_02\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_03\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_04\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_05\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_06\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_07\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_08\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_09\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_10\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_YTD\" INTEGER CS_INT,\n"
			"	\"S_ORDER_CNT\" SMALLINT CS_INT,\n"
			"	\"S_REMOTE_CNT\" SMALLINT CS_INT,\n"
			"	\"S_DATA\" CHAR(50) CS_FIXEDSTRING,\n"
			"	\"S_SU_SUPPKEY\" INTEGER CS_INT,\n"
   */
  val stockSch: TellSchema = new TellSchema()
  stockSch.addField(Schema.FieldType.INT, "S_I_ID", false)
  stockSch.addField(Schema.FieldType.INT, "S_W_ID", false)
  stockSch.addField(Schema.FieldType.SMALLINT, "S_QUANTITY", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_01", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_02", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_03", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_04", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_05", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_06", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_07", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_08", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_09", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DIST_10", false)
  stockSch.addField(Schema.FieldType.INT, "S_YTD", false)
  stockSch.addField(Schema.FieldType.SMALLINT, "S_ORDER_CNT", false)
  stockSch.addField(Schema.FieldType.SMALLINT, "S_REMOTE_CNT", false)
  stockSch.addField(Schema.FieldType.TEXT, "S_DATA", false)
  stockSch.addField(Schema.FieldType.INT, "S_SU_SUPPKEY", false)

  /**
   * \"ITEM\" (\n"
			"	\"I_ID\" INTEGER CS_INT,\n"
			"	\"I_IM_ID\" SMALLINT CS_INT,\n"
			"	\"I_NAME\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"I_PRICE\" DECIMAL(5,2) CS_FIXED,\n"
			"	\"I_DATA\" CHAR(50) CS_FIXEDSTRING,\n"
   */
  val itemSch: TellSchema = new TellSchema()
  itemSch.addField(Schema.FieldType.INT, "I_ID", false)
  itemSch.addField(Schema.FieldType.SMALLINT, "I_IM_ID", false)
  itemSch.addField(Schema.FieldType.TEXT, "I_NAME", false)
  itemSch.addField(Schema.FieldType.DOUBLE, "I_PRICE", false)
  itemSch.addField(Schema.FieldType.TEXT, "I_DATA", false)
}
