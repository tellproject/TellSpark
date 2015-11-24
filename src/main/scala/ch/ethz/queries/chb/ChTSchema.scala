package ch.ethz.queries.chb

import ch.ethz.tell.{TSchema, Transaction}


//TODO: probably, this class becomes obsolete at some point, remove it!

/**
 * Created by marenato on 10.11.15.
 * Rewritten by braunl on 17.11.15
 */
object ChTSchema  extends Serializable{

  var warehouseSch: TSchema = null
  var districtSch: TSchema = null
  var customerSch: TSchema = null
  var historySch: TSchema = null
  var newOrderSch: TSchema = null
  var orderSch: TSchema = null
  var orderLineSch: TSchema = null
  var itemSch: TSchema = null
  var stockSch: TSchema = null
  var regionSch: TSchema = null
  var nationSch: TSchema = null
  var supplierSch: TSchema = null

  def init_schem(transaction: Transaction) = {
    var tellSchema = transaction.schemaForTable("warehouse")
    warehouseSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("district")
    districtSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("customer")
    customerSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("history")
    historySch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("new-order")
    newOrderSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("order")
    orderSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("order-line")
    orderLineSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("item")
    itemSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("stock")
    stockSch= new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("region")
    regionSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("nation")
    nationSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("supplier")
    supplierSch = new TSchema(tellSchema)

  }


}
