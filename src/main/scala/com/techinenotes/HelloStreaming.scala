package example

import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object HelloStreaming extends StreamingDriver {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    println(greeting)
    println(spark.sparkContext.version)

    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.18.0.2:6667")   // comma separated list of broker:host
      .option("subscribe", "test-topic")    // comma separated list of topics
      .option("startingOffsets", "earliest") // read data from the end of the stream {earliest, latest, or json string {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}}
      .load()

    val dsKafka = kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    dsKafka.writeStream.format("orc")
      .option("compression", "snappy")
      .option("path", "/user/maria_dev/bigdata-streaming")
      .option("checkpointLocation", "/user/maria_dev/bigdata-checkpoint")
      .outputMode("append")
      //      .trigger(Trigger.Continuous("1 second"))  // only change in query
      .trigger(Trigger.ProcessingTime("10 seconds")) // check for files every 10s
      .start().awaitTermination()
  }
}

trait StreamingDriver {
  lazy val appName = "Spark Streaming App"
  lazy val greeting: String = "hello streaming"
}