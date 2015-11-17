package ch.ethz.queries

import ch.ethz.tell.{TSchema, Schema}

/**
 * Created by marenato on 10.11.15.
 */
object ChTSchema {
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
  val warehouseSch: TSchema = new TSchema()
  warehouseSch.addField(Schema.FieldType.INT, "w_id", false)
  warehouseSch.addField(Schema.FieldType.DOUBLE, "w_tax", false)
  warehouseSch.addField(Schema.FieldType.DOUBLE, "w_ytd", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "w_name", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "w_street_1", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "w_street_2", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "w_city", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "w_state", false)
  warehouseSch.addField(Schema.FieldType.TEXT, "w_zip", false)

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
  val districtSch: TSchema = new TSchema()
  districtSch.addField(Schema.FieldType.SMALLINT, "d_id", false)
  districtSch.addField(Schema.FieldType.INT, "d_w_id", false)
  districtSch.addField(Schema.FieldType.INT, "d_next_o_id", false)
  districtSch.addField(Schema.FieldType.DOUBLE, "d_tax", false)
  districtSch.addField(Schema.FieldType.DOUBLE, "d_ytd", false)
  districtSch.addField(Schema.FieldType.TEXT, "d_name", false)
  districtSch.addField(Schema.FieldType.TEXT, "d_street_1", false)
  districtSch.addField(Schema.FieldType.TEXT, "d_street_2", false)
  districtSch.addField(Schema.FieldType.TEXT, "d_city", false)
  districtSch.addField(Schema.FieldType.TEXT, "d_state", false)
  districtSch.addField(Schema.FieldType.TEXT, "d_zip", false)

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
  val customerSch: TSchema = new TSchema()
  customerSch.addField(Schema.FieldType.SMALLINT, "c_id", false)
  customerSch.addField(Schema.FieldType.SMALLINT, "c_d_id", false)
  customerSch.addField(Schema.FieldType.SMALLINT, "c_payment_cnt", false)
  customerSch.addField(Schema.FieldType.SMALLINT, "c_delivery_cnt", false)
  customerSch.addField(Schema.FieldType.INT, "c_n_nationkey", false)
  customerSch.addField(Schema.FieldType.INT, "c_w_id", false)
  customerSch.addField(Schema.FieldType.BIGINT, "c_since", false)
  customerSch.addField(Schema.FieldType.DOUBLE, "c_credit_lim", false)
  customerSch.addField(Schema.FieldType.DOUBLE, "c_discount", false)
  customerSch.addField(Schema.FieldType.DOUBLE, "c_balance", false)
  customerSch.addField(Schema.FieldType.DOUBLE, "c_ytd_payment", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_first", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_middle", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_last", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_street_1", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_street_2", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_city", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_state", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_zip", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_phone", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_credit", false)
  customerSch.addField(Schema.FieldType.TEXT, "c_data", false)


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
  val historySch: TSchema = new TSchema()
  historySch.addField(Schema.FieldType.SMALLINT, "h_c_id", false)
  historySch.addField(Schema.FieldType.SMALLINT, "h_c_d_id", false)
  historySch.addField(Schema.FieldType.SMALLINT, "h_d_id", false)
  historySch.addField(Schema.FieldType.INT, "h_w_id", false)
  historySch.addField(Schema.FieldType.INT, "h_c_w_id", false)
  historySch.addField(Schema.FieldType.BIGINT, "h_date", false)
  historySch.addField(Schema.FieldType.DOUBLE, "h_amount", false)
  historySch.addField(Schema.FieldType.TEXT, "h_data", false)

  /**
   * "NEWORDER\" (\n"
			"	\"NO_O_ID\" INTEGER CS_INT,\n"
			"	\"NO_D_ID\" TINYINT CS_INT,\n"
			"	\"NO_W_ID\" INTEGER CS_INT,\n"
   */
  val newOrderSch: TSchema = new TSchema()
  newOrderSch.addField(Schema.FieldType.SMALLINT, "no_d_id", false)
  newOrderSch.addField(Schema.FieldType.INT, "no_o_id", false)
  newOrderSch.addField(Schema.FieldType.INT, "no_w_id", false)

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
  val orderSch: TSchema = new TSchema()
  orderSch.addField(Schema.FieldType.SMALLINT, "o_d_id", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "o_c_id", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "o_carrier_id", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "o_ol_cnt", false)
  orderSch.addField(Schema.FieldType.SMALLINT, "o_all_local", false)
  orderSch.addField(Schema.FieldType.INT, "o_id", false)
  orderSch.addField(Schema.FieldType.INT, "o_w_id", false)
  orderSch.addField(Schema.FieldType.BIGINT, "o_entry_d", false)


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
  val orderLineSch: TSchema = new TSchema()

  orderLineSch.addField(Schema.FieldType.SMALLINT, "ol_d_id", false)
  orderLineSch.addField(Schema.FieldType.SMALLINT, "ol_number", false)
  orderLineSch.addField(Schema.FieldType.SMALLINT, "ol_quantity", true)
  orderLineSch.addField(Schema.FieldType.INT, "ol_o_id", false)
  orderLineSch.addField(Schema.FieldType.INT, "ol_w_id", false)
  orderLineSch.addField(Schema.FieldType.INT, "ol_i_id", true)
  orderLineSch.addField(Schema.FieldType.INT, "ol_supply_w_id", true)
  orderLineSch.addField(Schema.FieldType.BIGINT, "ol_delivery_d", true)
  orderLineSch.addField(Schema.FieldType.DOUBLE, "ol_amount", true)
  orderLineSch.addField(Schema.FieldType.TEXT, "ol_dist_info", true)

  /**
   *\"NATION\" (\n"
			"	\"N_NATIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"N_NAME\" CHAR(25) CS_FIXEDSTRING NOT NULL,\n"
			"	\"N_REGIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"N_COMMENT\" CHAR(152) CS_FIXEDSTRING NOT NULL,\n"
   */
  val nationSch: TSchema = new TSchema()
  nationSch.addField(Schema.FieldType.SMALLINT, "n_nationkey", false)
  nationSch.addField(Schema.FieldType.SMALLINT, "n_regionkey", false)
  nationSch.addField(Schema.FieldType.TEXT, "n_name", false)
  nationSch.addField(Schema.FieldType.TEXT, "n_comment", false)

  /**
   * \"REGION\" (\n"
			"	\"R_REGIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"R_NAME\" CHAR(55) CS_FIXEDSTRING NOT NULL,\n"
			"	\"R_COMMENT\" CHAR(152) CS_FIXEDSTRING NOT NULL,\n"
   */
  val regionSch: TSchema = new TSchema()
  regionSch.addField(Schema.FieldType.SMALLINT, "r_regionkey", false)
  regionSch.addField(Schema.FieldType.TEXT, "r_name", false)
  regionSch.addField(Schema.FieldType.TEXT, "r_comment", false)

  /** "SUPPLIER\" (\n"
			"	\"SU_SUPPKEY\" SMALLINT CS_INT NOT NULL,\n"
			"	\"SU_NAME\" CHAR(25) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_ADDRESS\" CHAR(40) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_NATIONKEY\"TINYINT CS_INT NOT NULL,\n"
			"	\"SU_PHONE\" CHAR(15) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_ACCTBAL\" DECIMAL(12,2) CS_FIXED NOT NULL,\n"
			"	\"SU_COMMENT\" CHAR(101) CS_FIXEDSTRING NOT NULL,\n"
   */
  val supplierSch: TSchema = new TSchema()
  supplierSch.addField(Schema.FieldType.SMALLINT, "su_suppkey", false)
  supplierSch.addField(Schema.FieldType.SMALLINT, "su_nationkey", false)
  supplierSch.addField(Schema.FieldType.DOUBLE, "su_acctbal", false)
  supplierSch.addField(Schema.FieldType.TEXT, "su_name", false)
  supplierSch.addField(Schema.FieldType.TEXT, "su_address", false)
  supplierSch.addField(Schema.FieldType.TEXT, "su_phone", false)
  supplierSch.addField(Schema.FieldType.TEXT, "su_comment", false)

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
  val stockSch: TSchema = new TSchema()
  stockSch.addField(Schema.FieldType.SMALLINT, "s_w_id", false)
  stockSch.addField(Schema.FieldType.SMALLINT, "s_order_cnt", false)
  stockSch.addField(Schema.FieldType.SMALLINT, "s_remote_cnt", false)
  stockSch.addField(Schema.FieldType.INT, "s_i_id", false)
  stockSch.addField(Schema.FieldType.INT, "s_ytd", false)
  stockSch.addField(Schema.FieldType.INT, "s_quantity", false)
  stockSch.addField(Schema.FieldType.INT, "s_su_suppkey", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_01", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_02", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_03", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_04", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_05", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_06", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_07", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_08", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_09", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_dist_10", false)
  stockSch.addField(Schema.FieldType.TEXT, "s_data", false)

  /**
   * \"ITEM\" (\n"
			"	\"I_ID\" INTEGER CS_INT,\n"
			"	\"I_IM_ID\" SMALLINT CS_INT,\n"
			"	\"I_NAME\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"I_PRICE\" DECIMAL(5,2) CS_FIXED,\n"
			"	\"I_DATA\" CHAR(50) CS_FIXEDSTRING,\n"
   */
  val itemSch: TSchema = new TSchema()
  itemSch.addField(Schema.FieldType.SMALLINT, "i_im_id", false)
  itemSch.addField(Schema.FieldType.INT, "i_id", false)
  itemSch.addField(Schema.FieldType.DOUBLE, "i_price", false)
  itemSch.addField(Schema.FieldType.TEXT, "i_name", false)
  itemSch.addField(Schema.FieldType.TEXT, "i_data", false)
}
