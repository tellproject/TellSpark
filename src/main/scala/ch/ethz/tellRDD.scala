package ch.ethz;

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * RDD class for connecting to TellStore
 */
class tellRDD [T: ClassTag]( @transient var sc: SparkContext,
                             @transient var deps: Seq[Dependency[_]])
  extends RDD[T](sc, deps) with Logging {



  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    //TODO for each partition registered, get the customer values out

    null
  }


  override protected def getPartitions: Array[Partition] = {
    //TODO get the number of blocks from tell connection manager
    val array = new Array[Partition](TellClient.nPartitions)
    val memLocs = TellClient.getMemLocations()
    (1 to TellClient.nPartitions).map(pos => {
      array(pos) = new tellPartition(pos, memLocs(pos))
    })
    array
  }
}
