package com.techinenotes

import org.apache.spark.sql.{SaveMode, SparkSession}

object Hello extends BatchDriver {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    println(greeting)
    println(spark.sparkContext.version)

    val infoDF = spark.read.format("csv").load("/user/maria_dev/info.csv")
    infoDF.write.format("orc").mode(SaveMode.Overwrite).save("/user/maria_dev/bigdata")
  }
}

trait BatchDriver {
  lazy val appName = "Spark Batch App"
  lazy val greeting: String = "hello batch"
}
