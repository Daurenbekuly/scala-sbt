package com.example.scala

import org.apache.spark.sql.functions.col

object IcebergRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef()

    val df = spark.table("rest.demo.kafka_users").orderBy(col("age").desc)
    df.show()

    spark.stop()
  }
}

