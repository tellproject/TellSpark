package ch.ethz.tell

import ch.ethz.TellClientFactory
import ch.ethz.tell.Schema.FieldType
import org.apache.spark.{SparkContext, _}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * RDD class for connecting to TellStore
 */
class TRDD [T: ClassTag]( @transient var sc: SparkContext,
                             @transient var deps: Seq[Dependency[_]])
  extends RDD [T](sc, deps) with Logging {

  // Tell schema
  var tSchema: TSchema = null
  // Tell table
  var tTable: String = ""
  // Tell query 
  var tQuery: ScanQuery = null
  // Tell context
  var tContext: TSparkContext = null

  def this(@transient scc: TSparkContext, tbl: String, qry: ScanQuery, sch: TSchema) = {
    this(scc.sparkContext, Nil)
    tSchema = sch
    tTable = tbl
    tQuery = qry
    tContext = scc
  }

  def this(@transient oneParent: TRDD[_]) = {
    this(oneParent.context, List(new OneToOneDependency(oneParent)))
  }

  def getIterator(theSplit: TPartition[T]): Iterator[T] = {
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>theSplit")
    //println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>theSplit" + theSplit.toString)
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
        if (offset == len) {
          keepGoing = theSplit.scanIt.next()
          len = theSplit.scanIt.length()
          addr = theSplit.scanIt.address()
          offset = 0
        }
        keepGoing
      }

      override def next(): T = {
//       if (offset == len) {
//          keepGoing = theSplit.scanIt.next()
//          len = theSplit.scanIt.length()
//          addr = theSplit.scanIt.address()
//          offset = 0
//       }
        if (offset <= len) {
          res = getRecord(addr, offset)
          offset = res._1
          cnt += 1
          //println(">>>>>>>>cnt:" + cnt + "<<<<<<" + ">>>>>>offset:" + offset + "<<<<<<len:" + len + ">>>>>>>")
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
    val rec:TRecord = new TRecord(tSchema, new Array[Any](tSchema.cnt))
    // fixed size fields
    for (fieldType:Schema.FieldType <- tSchema.fixedSizeFields) {
      fieldType match {
        case FieldType.SMALLINT =>
	  val vv = unsafe.getShort(addr + off)
          rec.setField(fieldCnt, unsafe.getShort(addr + off))
          fieldCnt += 1
          off += 2
        case FieldType.INT | FieldType.FLOAT =>
	  val vv = unsafe.getInt(addr+off)
          rec.setField(fieldCnt, unsafe.getInt(addr + off))
          fieldCnt += 1
          off += 4
        case FieldType.BIGINT | FieldType.DOUBLE =>
	  val vv = unsafe.getLong(addr + off)
          rec.setField(fieldCnt, unsafe.getLong(addr + off))
          fieldCnt += 1
          off += 8;
      }
    }
    // variable sized fields
    for (fieldType:Schema.FieldType <- tSchema.varSizeFields) {
      if (off % 4 != 0) off += 4 - (off % 4)
      var ln = unsafe.getInt(addr + off);
      off += 4;
      val str = readString(unsafe, addr + off, ln);
      rec.setField(fieldCnt, str)
      fieldCnt += 1
      off += ln;
    }
    // aligning the next record
    if (off % 8 != 0) off += 8 - (off % 8)
    (off, rec.asInstanceOf[T])
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
    println("============POST COMMIT=================") 
    Array.concat(results: _*)
  }

  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    //TODO move the client creation somewhere else?
    TellClientFactory.setConf(
      tContext.storageMng.value,
      tContext.commitMng.value,
      tContext.chNumber.value,
      tContext.chSize.value)
    println("=================== COMPUTE :storageMng: ========= " + tContext.storageMng.value)
    println("=================== COMPUTE :trxId: ========= " + tContext.broadcastTc.value)
    val trxId = tContext.broadcastTc.value
    TellClientFactory.startTransaction(trxId)
    println("+++++++++++++++++++++++++++++++++++++")
    getIterator(split.asInstanceOf[TPartition[T]])
  }

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](TellClientFactory.chNumber)
//    TellClientFactory.startTransaction()
    //TODO move the client creation somewhere else?
    TellClientFactory.setConf(
      tContext.storageMng.value,
      tContext.commitMng.value,
      tContext.chNumber.value,
      tContext.chSize.value)

    val trxId = tContext.broadcastTc.value

    println("=================== GET_PARTITION :trxId: ========= " + trxId)

    (0 to TellClientFactory.chNumber -1).map(pos => {
      array(pos) = new TPartition(pos,
        TellClientFactory.trx.scan(new ScanQuery(TellClientFactory.chNumber, pos, tQuery), tTable))
      println("PARTITION>>>" + array(pos).toString)
    })
    array
  }
}
