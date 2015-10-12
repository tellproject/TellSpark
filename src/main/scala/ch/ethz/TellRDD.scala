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
  var tSchema: Schema = null
  // Tell table
  var tTable: String = ""
  // Tell query 
  var tQuery: ScanQuery = null

  def this(sc: SparkContext, tbl: String, qry: ScanQuery, sch: Schema) = {
    this(sc, null)
    tSchema = sch
    tTable = tbl
    tQuery = qry
  }

  def getIterator(theSplit: TellPartition[T]): Iterator[T] = {
    val it = new Iterator[T] {
      // TODO a better way to map this?
      var elems = mutable.Queue[T]()
      var counter = 0

      override def hasNext: Boolean = {
        elems.isEmpty
      }

      override def next(): T = {
        if (elems.front == null) {
          if (theSplit.scanIt.next()) {
            theSplit.scanIt.address()
            theSplit.scanIt.length()
            elems ++= getRecords(theSplit.scanIt.address(), theSplit.scanIt.length())
          }
        }
        elems.dequeue()
      }
    }
    it
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
      for (fieldType:Schema.FieldType <- tSchema.variableSizedFields) {
        var ln = unsafe.getInt(addr + offset);
        offset += 4;
        var str = readString(unsafe, addr + offset, ln);
        if (str.length() != 94 && str.length() != 147)
          throw new IllegalStateException("Error while reading expected record");
        tmpCustomer.setField(fieldCnt, str)
        fieldCnt += 1
        offset += ln;
      }
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

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    //TODO for each partition registered, get the customer values out
    getIterator(split.asInstanceOf[TellPartition[T]])
  }

  def getUnsafe(): sun.misc.Unsafe = {
    val singleoneInstanceField: Field = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    singleoneInstanceField.setAccessible(true)
    singleoneInstanceField.get(null).asInstanceOf[sun.misc.Unsafe]
  }

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](TellClientFactory.chNumber)
//    println("@@")
//    val u = getUnsafe()
    println("@@")
    val clientManager = new ClientManager("192.168.0.11:7242", "192.168.0.11:7241", 4, 50000)
    println("@@")
    TellClientFactory.startTransaction()

    (0 to TellClientFactory.chNumber -1).map(pos => {
      //TODO do range querying

      println("=1=================================" + pos)
      array(pos) = new TellPartition(pos, TellClientFactory.trx.scan(new ScanQuery, tTable, null))
      println("=2=================================")
    })
    array
  }
}
