package ch.ethz.queries.tpch

import ch.ethz.tell.{TSchema, Transaction}

/**
 * Wrapper for getting the right schema from Tell
 */
object TpchTSchema  extends Serializable {

  var partSch: TSchema = null
  var supplierSch: TSchema = null
  var partSuppSch: TSchema = null
  var customerSch: TSchema = null
  var orderSch: TSchema = null
  var lineItemSch: TSchema = null
  var nationSch: TSchema = null
  var regionSch: TSchema = null

  def init_schema(transaction: Transaction) = {
    var tellSchema = transaction.schemaForTable("part")
    partSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("supplier")
    supplierSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("partupp")
    partSuppSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("customer")
    customerSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("order")
    orderSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("lineitem")
    lineItemSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("region")
    regionSch = new TSchema(tellSchema)

    tellSchema = transaction.schemaForTable("nation")
    nationSch = new TSchema(tellSchema)
  }


}
