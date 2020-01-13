import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.MultiOutputRDD.rddToMultiOutputRDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class MultiOutputRDDTest extends FunSuite {

  test("multiOutput") {

    val spark = SparkSession.builder
      .appName("multiOutput")
      .master("local[4]")
      .config("spark.yarn.maxAppAttempts", 3)
      .getOrCreate()

    // write
    val result = spark.sparkContext
      .parallelize(
        List(
          ("1", "1"),
          ("1", "1"),
          ("1", "1"),
          ("2", "2"),
          ("2", "2"),
          ("3", "3")),
        2)
      .map(data => (data._1, (NullWritable.get(), new Text().set(data._2))))
      .saveAsMultiOutputFile(
        "output",
        classOf[NullWritable],
        classOf[Text],
        classOf[TextOutputFormat[NullWritable, Text]],
        spark.sparkContext.hadoopConfiguration)
  }
}
