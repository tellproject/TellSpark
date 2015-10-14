package ch.ethz;

import ch.ethz.tell.Schema.FieldType
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

import ch.ethz.tell.{Unsafe, ScanQuery, Schema}

/**
 * RDD class for connecting to TellStore
 */
class TellRDD [T: ClassTag]( @transient var sc: SparkContext,
                             @transient var deps: Seq[Dependency[_]])
  extends RDD[T](sc, deps) with Logging {

  // Tell schema
  var tSchema: TellSchema = null
  // Tell table
  var tTable: String = ""
  // Tell query 
  var tQuery: ScanQuery = null

  def this(@transient sc: SparkContext, tbl: String, qry: ScanQuery, sch: TellSchema) = {
    this(sc, Nil)
    tSchema = sch
    tTable = tbl
    tQuery = qry
  }

  def getIterator(theSplit: TellPartition[T]): Iterator[T] = {
    val it = new Iterator[T] {
      // TODO a better way to map this?
      var offset = 0L
      var cnt = 0
      var cnt1 = 0
      var keepGoing = theSplit.scanIt.next()
      var len = theSplit.scanIt.length()
      var addr = theSplit.scanIt.address()
      var res:(Long, T) = null

      override def hasNext: Boolean = {
        keepGoing
      }

      override def next(): T = {
        var tmpCustomer:Customer = new Customer
        if (offset == len) {
          keepGoing = theSplit.scanIt.next()
          len = theSplit.scanIt.length()
          addr = theSplit.scanIt.address()
          offset = 0
        }
        if (offset != len) {
          res = getRecord(addr, offset)// new Customer
          offset = res._1
          cnt += 1
        }
        res._2
      }
    }

    it
  }

  def getRecord(addr: Long, offset: Long): (Long, T) = {

    var fieldCnt = 0
    val unsafe: sun.misc.Unsafe = Unsafe.getUnsafe()
    var off = offset
    off += 8
    val tmpCustomer = new Customer
    // fixed size fields
    for (fieldType:Schema.FieldType <- tSchema.fixedSizeFields) {


      fieldType match {
        case FieldType.SMALLINT =>

          tmpCustomer.setField(fieldCnt, unsafe.getShort(addr + off))
          fieldCnt += 1
          off += 2
        case FieldType.INT | FieldType.FLOAT =>

          tmpCustomer.setField(fieldCnt, unsafe.getInt(addr + off))
          fieldCnt += 1
          off += 4
        case FieldType.BIGINT | FieldType.DOUBLE =>

          tmpCustomer.setField(fieldCnt, unsafe.getLong(addr + off))
          fieldCnt += 1
          off += 8;
      }
    }
    fieldCnt += 1
    // variable sized fields
    for (fieldType:Schema.FieldType <- tSchema.varSizeFields) {
      var ln = unsafe.getInt(addr + off);
      off += 4;
      val str = readString(unsafe, addr + off, ln);
      tmpCustomer.setField(fieldCnt, str)
      fieldCnt += 1
      off += ln;
    }
    // aligning the next record
    if (off % 8 != 0) off += 8 - (off % 8)
    (off, tmpCustomer.asInstanceOf[T])
  }

  def readString(u: sun.misc.Unsafe, add: Long, length: Int): String = {
    val str = new Array[Byte](length)
    var i = 0
    for (i <- 0 to length-1){
      str(i) = u.getByte(add + i)
    }
    new String(str, "UTF-8")
  }

  /**
   * Returns an array with all elements in this RDD
   * @return
   */
  override def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    TellClientFactory.trx.commit()
    Array.concat(results: _*)
  }

  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    //TODO for each partition registered, get the customer values out
    getIterator(split.asInstanceOf[TellPartition[T]])
  }

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](TellClientFactory.chNumber)
    val proj:Array[Short] = null
    TellClientFactory.startTransaction()

    (0 to TellClientFactory.chNumber -1).map(pos => {
      //TODO do range querying
      array(pos) = new TellPartition(pos, TellClientFactory.trx.scan(new ScanQuery, tTable, proj))
    })
    array
  }
}
