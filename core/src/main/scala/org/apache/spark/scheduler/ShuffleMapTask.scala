/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                  at the same time for a barrier stage.
 */
// 一个ShuffleMapTask会将一个RDD元素，切分为多个bucket
// 基于一个在ShuffleDependency中指定的partition，默认就是HashPartition
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId, isBarrier)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, new Properties, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  // 非常重要的一点，就是shuffleMapTask的runTask方法，有MapStatus返回值
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    // 对task要处理的rdd相关的数据，做一些反序列化操作
    // 这个rdd关键的问题是怎么拿到的？
    // 因为多个task运行在多个Executor中，都是并行或者并发执行的
    // 可能会不在一个地方，但是一个stage的task，其实要处理的rdd是一样的
    // 这里task通过broadcast variable（广播变量）直接拿到
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 获取ShuffleManager
      val manager = SparkEnv.get.shuffleManager
      // 从ShuffleManager中获取ShuffleWriter
      // registerShuffle的registerShuffle方法决定getWriter方法中返回调用哪个manager
      // manager包括unsafeShuffleHandle、bypassMergeSortHandle和SortShuffleWriter
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // 核心处理代码是该句
      // 首先调用了rdd的iterator方法，并且传入了当前task要处理哪个partition
      // 核心的逻辑就在rdd的iterator方法中，在这里就实现了针对rdd的某个partition执行我们自己定义算子或者函数
      // 执行完了我们自己定义的算子或者函数，相当于对rdd的partition执行了处理，那么就会有返回的数据
      // 返回的数据，都是通过ShuffleWriter，经过HashPartition进行分区之后，写入自己对应的分区bucket
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      // 最后返回结果MapStatus
      // MapStatus里面封装了BlockManager（存储了ShuffleMapTask计算后的数据）的相关存储信息（包含了数据的location和size等元数据信息）
      // BlockManager是Spark底层的内存、数据、磁盘管理的组件，后续shuffle中会剖析BlockManager
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
