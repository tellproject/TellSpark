package ch.ethz.queries

import ch.ethz.tell
import ch.ethz.tell._

import ch.ethz.tell.ScanQuery
/**
Query22
select	 substr(c_state,1,1) as country,
	 count(*) as numcust,
	 sum(c_balance) as totacctbal
from	 customer
where	 substr(c_phone,1,1) in ('1','2','3','4','5','6','7')
	 and c_balance > (select avg(c_BALANCE)
			  from 	 customer
			  where  c_balance > 0.00
			 	 and substr(c_phone,1,1) in ('1','2','3','4','5','6','7'))
	 and not exists (select *
			 from	orders
			 where	o_c_id = c_id
			     	and o_w_id = c_w_id
			    	and o_d_id = c_d_id)
group by substr(c_state,1,1)
order by substr(c_state,1,1)
  */
class Q22 extends ChQuery {

  /**
    * implemented in children classes and hold the actual query
    */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // assumption, columns start with 0 and have the order defined in CHQuery.scala. Not sure whether this assumption holds...
    val phoneIndex: Short = 11
    val balanceIndex: Short = 16;

    // convert an RDDs to a DataFrames

    // first subquery

    val query = new tell.ScanQuery()


    val clause1 = new query.CNFCLause() // clause for substring matching
    // assuming that the shorts will be converted to prefix-strings once we will implement the LIKE predicates
    for (i <- 1 to 7)
      clause1.addPredicate(ScanQuery.CmpType.LIKE, phoneIndex, PredicateType.create(String.valueOf(i)))

    query.addSelection(clause1)

    val clause2 = new query.CNFCLause() // clause for balance greater 0.0
    clause2.addPredicate(ScanQuery.CmpType.GREATER, balanceIndex, PredicateType.create(0.0))
    query.addSelection(clause2)

    query.addProjection(3);

    val customer = new TRDD[TRecord](scc, "customer", query, ChTSchema.customerSch).map(r => {
      Customer(r.getField("C_ID").asInstanceOf[Int],0,0,"","","","","","","","","",0L,"",0.0,0.0,0.0,0.0,0,0,"",0)
    }).toDF()

    //ToDo push downs
    val res = customer.filter($"OL_DELIVERY_D" > "2007-01-02")
      .groupBy($"OL_NUMBER")
      .agg(sum($"OL_AMOUNT"),
        sum($"OL_QUANTITY"),
        avg($"OL_QUANTITY"),
        avg($"OL_AMOUNT"),
        count($"OL_NUMBER"))
      .sort($"OL_NUMBER")
    //outputDF(res)
  }
}