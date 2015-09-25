package ch.ethz;

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * RDD class for connecting to TellStore
 */
class TellRDD [T: ClassTag]( @transient var sc: SparkContext,
                             @transient var deps: Seq[Dependency[_]])
  extends RDD[T](sc, deps) with Logging {


  def getIterator(theSplit: TellPartition[T]): Iterator[T] = {
    val it = new Iterator[T] {
      var counter = 0

      override def hasNext: Boolean = {
        // TODO the number of records should also be specified with some byes
        counter < 1
      }

      override def next(): T = {
        val memAddrs = theSplit.memAddr
        val c: Customer = Customer.deserialize(memAddrs)
        counter += 1
        c.asInstanceOf[T]
      }
    }
    it
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    //TODO for each partition registered, get the customer values out
    getIterator(split.asInstanceOf[TellPartition[T]])
  }


  override protected def getPartitions: Array[Partition] = {
    //TODO get the number of blocks from tell connection manager
    val array = new Array[Partition](TellClient.nPartitions)
    val memLocs = TellClient.getMemLocations()
    (0 to TellClient.nPartitions -1).map(pos => {
      array(pos) = new TellPartition(pos, memLocs(pos))
    })
    array
  }
}
