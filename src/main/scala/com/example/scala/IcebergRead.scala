package com.example.scala

import org.apache.spark.sql.functions.col

object IcebergRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef()

    spark
      .table("rest.demo.kafka_users")
      .orderBy(col("age").desc)
      .show()

    spark.stop()
  }
}

