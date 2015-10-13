package ch.ethz;

import ch.ethz.tell.Schema.FieldType
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import java.lang.reflect.Field


import ch.ethz.tell.{ClientManager, Unsafe, ScanQuery, Schema}

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
      var keepGoing = theSplit.scanIt.next()
      var len = theSplit.scanIt.length()
      var addr = theSplit.scanIt.address()

      override def hasNext: Boolean = {
        println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$4" + keepGoing)
        keepGoing
      }

      override def next(): T = {
        var tmpCustomer:Customer = null
        if (offset == len) {
          keepGoing = theSplit.scanIt.next()
          len = theSplit.scanIt.length()
          addr = theSplit.scanIt.address()
          offset = 0
        }
        if (offset != len) {
          tmpCustomer = new Customer
          offset = len
          cnt += 1
        }
        tmpCustomer.asInstanceOf[T]
      }
    }

    it
  }

  def getRecord(addr: Long, len: Long, offset: Long): (Long, T) = {
    var fieldCnt = 0
    val unsafe: sun.misc.Unsafe = Unsafe.getUnsafe()
    var off = offset
    off += 8
    var tmpCustomer = new Customer
    // fixed size fields
    for (fieldType:Schema.FieldType <- tSchema.fixedSizeFields) {
      fieldType match {
        case FieldType.SMALLINT =>
        case FieldType.INT =>
        case FieldType.FLOAT =>
          off += 2L
          tmpCustomer.setField(fieldCnt, unsafe.getInt(addr + off))
          fieldCnt += 1
          off += 4
        case FieldType.BIGINT =>
        case FieldType.DOUBLE =>
          off  += 6;
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
      var str = readString(unsafe, addr + off, ln);
      if (str.length() != 94 && str.length() != 147)
        throw new IllegalStateException("Error while reading expected record");
      tmpCustomer.setField(fieldCnt, str)
      fieldCnt += 1
      if (off % 8 != 0) off += 8 - (off % 8)
      off += ln;
    }
    println("/////////////////////////////////////")
    println(tmpCustomer.toString)
    println("/////////////////////////////////////")
    (off, tmpCustomer.asInstanceOf[T])
  }

  def getRecords(addr: Long, len: Long): List[T] = {
    var offset = 0
    val unsafe: sun.misc.Unsafe = Unsafe.getUnsafe()
    var recs = ListBuffer[Customer]()

    while (offset != len) {
      var fieldCnt = 0
      var tmpCustomer = new Customer
      // fixed size fields
      for (fieldType:Schema.FieldType <- tSchema.fixedSizeFields) {
        fieldType match {
          case FieldType.SMALLINT =>
          case FieldType.INT =>
          case FieldType.FLOAT =>
            offset += 2
            tmpCustomer.setField(fieldCnt, unsafe.getInt(addr + offset))
            fieldCnt += 1
            offset += 4
          case FieldType.BIGINT =>
          case FieldType.DOUBLE =>
            offset += 6;
            tmpCustomer.setField(fieldCnt, unsafe.getLong(addr + offset))
            fieldCnt += 1
            offset += 8;
        }
      }
      fieldCnt += 1
      // variable sized fields
      for (fieldType:Schema.FieldType <- tSchema.varSizeFields) {
        var ln = unsafe.getInt(addr + offset);
        offset += 4;
        var str = readString(unsafe, addr + offset, ln);
        if (str.length() != 94 && str.length() != 147)
          throw new IllegalStateException("Error while reading expected record");
        tmpCustomer.setField(fieldCnt, str)
        fieldCnt += 1
        offset += ln;
      }
      println("/////////////////////////////////////")
      println(tmpCustomer.toString)
      println("/////////////////////////////////////")
      recs += tmpCustomer
    }
    null
  }

  def readString(u: sun.misc.Unsafe, add: Long, length: Int): String = {
    val str = new Array[Byte](length)
    var i = 0
    for (i <- 1 to length){
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
    println("/////////////////////////////////////")
    val it = getIterator(split.asInstanceOf[TellPartition[T]])
    println("/////////////////////////////////////")
    it
  }

//  def getUnsafe(): sun.misc.Unsafe = {
//    val singleoneInstanceField: Field = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
//    singleoneInstanceField.setAccessible(true)
//    singleoneInstanceField.get(null).asInstanceOf[sun.misc.Unsafe]
//  }

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
