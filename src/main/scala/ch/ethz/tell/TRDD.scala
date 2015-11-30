package ch.ethz.tell

import ch.ethz.tell.Field.FieldType
import org.apache.spark.{SparkContext, _}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * RDD class for connecting to TellStore
 */
class TRDD [T: ClassTag]( @transient var sc: SparkContext,
                             @transient var deps: Seq[Dependency[_]])
  extends RDD [T](sc, deps) with Logging {

  // class logger
  val logger = LoggerFactory.getLogger(this.getClass)
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

  def getIterator(scanIt: ScanIterator): Iterator[T] = {
    logger.info("[TRDD] Iterating through a split")
    val it = new Iterator[T] {
      // TODO a better way to map this?
      var offset = 0L
      var cnt = 0
      var cnt1 = 0
      var keepGoing = scanIt.next()
      var len = 0L
      var addr = 0L
      if (keepGoing) {
        len = scanIt.length()
        addr = scanIt.address()
      }
      var res:(Long, T) = null

      override def hasNext: Boolean = {
        if (offset == len && keepGoing) {
          keepGoing = scanIt.next()
          if (keepGoing) {
            len = scanIt.length()
            addr = scanIt.address()
          }
          offset = 0
        }
        keepGoing
      }

      override def next(): T = {
        if (offset <= len) {
          res = getRecord(addr, offset)
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
    off += tSchema.headerLength
    val rec:TRecord = new TRecord(tSchema, new Array[Any](tSchema.getSize()))
    // fixed size fields
    for (fieldType:Field.FieldType <- tSchema.fixedSizeFields) {
      fieldType match {
        case FieldType.SMALLINT =>
          //val vv = unsafe.getShort(addr + off)
          rec.setField(fieldCnt.asInstanceOf[Short], unsafe.getShort(addr + off))
          fieldCnt += 1
          off += 2
        case FieldType.INT | FieldType.FLOAT =>
          //val vv = unsafe.getInt(addr+off)
          rec.setField(fieldCnt.asInstanceOf[Short], unsafe.getInt(addr + off))
          fieldCnt += 1
          off += 4
        case FieldType.BIGINT | FieldType.DOUBLE =>
          //val vv = unsafe.getLong(addr + off)
          rec.setField(fieldCnt.asInstanceOf[Short], unsafe.getLong(addr + off))
          fieldCnt += 1
          off += 8;
      }
    }
    // variable sized fields
    for (fieldType:Field.FieldType <- tSchema.varSizeFields) {
      if (off % 4 != 0) off += 4 - (off % 4)
      var ln = unsafe.getInt(addr + off);
      off += 4;
      val str = readString(unsafe, addr + off, ln);
      rec.setField(fieldCnt.asInstanceOf[Short], str)
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
    TClientFactory.trx.commit()
    logger.info("[TRDD] TellStore transaction has been committed.")
    Array.concat(results: _*)
  }

  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    //TODO move the client creation somewhere else?
    TClientFactory.setConf(
      tContext.storageMng.value,
      tContext.commitMng.value,
      tContext.chNumber.value,
      tContext.chSize.value)

    val trxId = tContext.broadcastTc.value
    TClientFactory.startTransaction(trxId)
    val theSplit = split.asInstanceOf[TPartition[T]]
    val scanIt = TClientFactory.trx.scan(new ScanQuery(TClientFactory.chNumber, theSplit.index, tQuery), tTable)
    logger.info("[TRDD] TellStore scanQuery using transactionId: %s".format(tContext.broadcastTc.value))
    getIterator(scanIt)
  }

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](TClientFactory.chNumber)
//    TellClientFactory.startTransaction()
    //TODO move the client creation somewhere else?
    TClientFactory.setConf(
      tContext.storageMng.value,
      tContext.commitMng.value,
      tContext.chNumber.value,
      tContext.chSize.value)

    val trxId = tContext.broadcastTc.value

    logger.info("[TRDD] Partition processing using trxId: %d".format(trxId))
    //TellClientFactory.trx.scan(new ScanQuery(TellClientFactory.chNumber, pos, tQuery), tTable))
    (1 to TClientFactory.chNumber).map(pos => {
      val scn = new ScanQuery(TClientFactory.chNumber, pos, tQuery)
      array(pos) = new TPartition(pos, scn, tTable)
      logger.info("[TRDD] Partition used: %s".format(array(pos).toString))
    })
    array
  }
}
