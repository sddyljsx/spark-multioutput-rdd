package org.apache.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SerializableConfiguration

import scala.reflect.ClassTag

class MultiOutputRDD[K, V](self: RDD[(String, (K, V))])(
    implicit kt: ClassTag[K],
    vt: ClassTag[V],
    ord: Ordering[K] = null) {

  def saveAsMultiOutputFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = self.context.hadoopConfiguration): Map[String, Long] = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val job = NewAPIHadoopJob.getInstance(hadoopConf)
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    job.setOutputFormatClass(outputFormatClass)
    val jobConfiguration = job.getConfiguration
    jobConfiguration.set("mapreduce.output.fileoutputformat.outputdir", path)
    saveAsMultiOutputDataset(jobConfiguration)
  }

  def saveAsMultiOutputDataset(conf: Configuration): Map[String, Long] = self.withScope {
    val config =
      new HadoopMapReduceMultiOutputWriteConfigUtil[K, V](new SerializableConfiguration(conf))
    SparkHadoopMultiWriter.write(self, config)
  }
}

object MultiOutputRDD {

  implicit def rddToMultiOutputRDD[K, V](rdd: RDD[(String, (K, V))])(
      implicit kt: ClassTag[K],
      vt: ClassTag[V],
      ord: Ordering[K] = null) = {
    new MultiOutputRDD(rdd)
  }
}
