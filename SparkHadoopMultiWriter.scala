package org.apache.spark

import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapreduce.task.{TaskAttemptContextImpl => NewTaskAttemptContextImpl}
import org.apache.hadoop.mapreduce.{
  TaskType,
  JobContext => NewJobContext,
  OutputFormat => NewOutputFormat,
  RecordWriter => NewRecordWriter,
  TaskAttemptContext => NewTaskAttemptContext,
  TaskAttemptID => NewTaskAttemptID
}
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.{
  FileCommitProtocol,
  HadoopMapReduceCommitProtocol,
  HadoopWriteConfigUtil,
  SparkHadoopWriterUtils
}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{SerializableConfiguration, Utils}

import scala.reflect.ClassTag

object SparkHadoopMultiWriter extends Logging {

  private val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256

  def write[K, V: ClassTag](
      rdd: RDD[(String, (K, V))],
      config: HadoopMapReduceMultiOutputWriteConfigUtil[K, V]): Map[String, Long] = {
    import SparkHadoopWriterUtils._

    // Extract context and configuration from RDD.
    val sparkContext = rdd.context
    val commitJobId = rdd.id

    // Set up a job.
    val jobTrackerId = createJobTrackerID(new Date())
    val jobContext = config.createJobContext(jobTrackerId, commitJobId)
    config.initOutputFormat(jobContext)

    // Assert the output format/key/value class is set in JobConf.
    config.assertConf(jobContext, rdd.conf)

    val committer = config.createCommitter(commitJobId)
    committer.setupJob(jobContext)

    // Try to write all RDD partitions as a Hadoop OutputFormat.
    try {
      val ret = sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[(String, (K, V))]) => {
          // SPARK-24552: Generate a unique "attempt ID" based on the stage and task attempt numbers.
          // Assumes that there won't be more than Short.MaxValue attempts, at least not concurrently.
          val attemptId = (context.stageAttemptNumber << 16) | context.attemptNumber

          executeTask(
            context = context,
            config = config,
            jobTrackerId = jobTrackerId,
            commitJobId = commitJobId,
            sparkPartitionId = context.partitionId,
            sparkAttemptNumber = attemptId,
            committer = committer,
            iterator = iter
          )
        }
      )
      val commitMessageList = ret.map(_._2)
      committer.commitJob(jobContext, commitMessageList)
      logInfo(s"Job ${jobContext.getJobID} committed.")
      ret
        .map(_._1)
        .reduce((x, y) =>
          (x.toList ++ y.toList).groupBy(_._1).map { case (k, v) => k -> v.map(_._2).sum })
    } catch {
      case cause: Throwable =>
        logError(s"Aborting job ${jobContext.getJobID}.", cause)
        committer.abortJob(jobContext)
        throw new SparkException("Job aborted.", cause)
    }
  }

  private def executeTask[K, V: ClassTag](
      context: TaskContext,
      config: HadoopMapReduceMultiOutputWriteConfigUtil[K, V],
      jobTrackerId: String,
      commitJobId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[(String, (K, V))]): (Map[String, Long], TaskCommitMessage) = {
    // Set up a task.
    val taskContext = config
      .createTaskAttemptContext(jobTrackerId, commitJobId, sparkPartitionId, sparkAttemptNumber)
    committer.setupTask(taskContext)

    val outputMetrics = initHadoopOutputMetrics(context)

    // Initiate the writer.
    config.initWriter(taskContext, sparkPartitionId)
    var recordsWritten = 0L

    // Write all rows in RDD partition.
    try {
      val ret = Utils.tryWithSafeFinallyAndFailureCallbacks {
        val partitionCountMap = scala.collection.mutable.Map[String, Long]()
        while (iterator.hasNext) {
          val pair = iterator.next()
          config.write(pair._1, pair._2)

          // Update bytes written metric every few records
          maybeUpdateOutputMetrics(outputMetrics, recordsWritten)
          recordsWritten += 1
          partitionCountMap.put(pair._1, partitionCountMap.getOrElse(pair._1, 0L) + 1L)
        }

        config.closeWriter(taskContext)
        (partitionCountMap.toMap, committer.commitTask(taskContext))
      }(catchBlock = {
        // If there is an error, release resource and then abort the task.
        try {
          config.closeWriter(taskContext)
        } finally {
          committer.abortTask(taskContext)
          logError(s"Task ${taskContext.getTaskAttemptID} aborted.")
        }
      })

      outputMetrics.setRecordsWritten(recordsWritten)

      ret
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }

  private def initHadoopOutputMetrics(context: TaskContext): OutputMetrics = {
    context.taskMetrics().outputMetrics
  }

  private def maybeUpdateOutputMetrics(outputMetrics: OutputMetrics, recordsWritten: Long): Unit = {
    if (recordsWritten % RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
      outputMetrics.setRecordsWritten(recordsWritten)
    }
  }

}

class HadoopMapReduceMultiOutputWriteConfigUtil[K, V: ClassTag](conf: SerializableConfiguration)
    extends HadoopWriteConfigUtil[K, V]
    with Logging {

  private var outputFormat: Class[_ <: NewOutputFormat[K, V]] = null
  private var recordWriters: ConcurrentHashMap[String, NewRecordWriter[K, V]] = null
  private var taskContext: NewTaskAttemptContext = null
  private val BASE_OUTPUT_NAME = "mapreduce.output.basename"
  private val PART = "part"

  private def getConf: Configuration = conf.value

  // --------------------------------------------------------------------------
  // Create JobContext/TaskAttemptContext
  // --------------------------------------------------------------------------

  override def createJobContext(jobTrackerId: String, jobId: Int): NewJobContext = {
    val jobAttemptId = new NewTaskAttemptID(jobTrackerId, jobId, TaskType.MAP, 0, 0)
    new NewTaskAttemptContextImpl(getConf, jobAttemptId)
  }

  override def createTaskAttemptContext(
      jobTrackerId: String,
      jobId: Int,
      splitId: Int,
      taskAttemptId: Int): NewTaskAttemptContext = {
    val attemptId =
      new NewTaskAttemptID(jobTrackerId, jobId, TaskType.REDUCE, splitId, taskAttemptId)
    new NewTaskAttemptContextImpl(getConf, attemptId)
  }

  // --------------------------------------------------------------------------
  // Create committer
  // --------------------------------------------------------------------------

  override def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol = {
    FileCommitProtocol
      .instantiate(
        className = classOf[HadoopMapReduceCommitProtocol].getName,
        jobId = jobId.toString,
        outputPath = getConf.get("mapreduce.output.fileoutputformat.outputdir")
      )
      .asInstanceOf[HadoopMapReduceCommitProtocol]
  }

  // --------------------------------------------------------------------------
  // Create writer
  // --------------------------------------------------------------------------

  override def initWriter(taskContext: NewTaskAttemptContext, splitId: Int): Unit = {
    this.recordWriters = new ConcurrentHashMap[String, NewRecordWriter[K, V]]()
    this.taskContext = taskContext
  }

  def write(baseOutputPath: String, pair: (K, V)): Unit = {
    require(recordWriters != null, "Must call createWriter before write.")
    getRecordWriter(taskContext, baseOutputPath).write(pair._1, pair._2)
  }

  override def write(pair: (K, V)): Unit = write("", pair)

  private def getRecordWriter(
      taskContext: NewTaskAttemptContext,
      baseFileName: String): NewRecordWriter[K, V] = { // look for record-writer in the cache
    var writer = recordWriters.get(baseFileName)
    // If not in cache, create a new one
    if (writer == null) { // get the record writer from context output format
      taskContext.getConfiguration.set(BASE_OUTPUT_NAME, if (baseFileName.isEmpty) { PART } else {
        baseFileName + "/" + PART
      })

      val taskFormat = getOutputFormat()
      // If OutputFormat is Configurable, we should set conf to it.
      taskFormat match {
        case c: Configurable => c.setConf(getConf)
        case _               => ()
      }

      writer = taskFormat
        .getRecordWriter(taskContext)
        .asInstanceOf[NewRecordWriter[K, V]]
      // add the record-writer to the cache
      recordWriters.put(baseFileName, writer)
    }
    writer
  }

  override def closeWriter(taskContext: NewTaskAttemptContext): Unit = {
    if (recordWriters != null) {
      import scala.collection.JavaConversions._
      for (writer <- recordWriters.values) {
        writer.close(taskContext)
      }
      recordWriters = null
    } else {
      logWarning("Writer has been closed.")
    }
  }

  // --------------------------------------------------------------------------
  // Create OutputFormat
  // --------------------------------------------------------------------------

  override def initOutputFormat(jobContext: NewJobContext): Unit = {
    if (outputFormat == null) {
      outputFormat = jobContext.getOutputFormatClass
        .asInstanceOf[Class[_ <: NewOutputFormat[K, V]]]
    }
  }

  private def getOutputFormat(): NewOutputFormat[K, V] = {
    require(outputFormat != null, "Must call initOutputFormat first.")

    outputFormat.newInstance()
  }

  // --------------------------------------------------------------------------
  // Verify hadoop config
  // --------------------------------------------------------------------------

  override def assertConf(jobContext: NewJobContext, conf: SparkConf): Unit = {
    if (SparkHadoopWriterUtils.isOutputSpecValidationEnabled(conf)) {
      getOutputFormat().checkOutputSpecs(jobContext)
    }
  }
}
