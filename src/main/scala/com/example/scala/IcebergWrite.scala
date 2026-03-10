package com.example.scala

import org.apache.spark.sql.functions.col

object IcebergWrite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef()

    val usersList = List(
      ("Alice", 35),
      ("Bob", 40),
      ("Charlie", 55),
      ("Alibi", 28),
      ("Serzhan", 25),
      ("Aizharkyn", 38)
    )

    spark.createDataFrame(usersList).toDF("name", "age")
      .coalesce(1)
      .writeTo("rest.demo.users")
      .using("iceberg")
      .createOrReplace()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.demo")

    spark.table("rest.demo.users")
      .filter(col("age") >= 40)
      .coalesce(1)
      .writeTo("rest.demo.adult_users")
      .using("iceberg")
      .createOrReplace()

    spark.stop()
  }
}

