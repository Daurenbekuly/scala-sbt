package com.example.scala

import org.apache.spark.sql.functions.col

object IcebergWrite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef("IcebergWrite")

    val usersList = List(
      ("Alice", 35),
      ("Bob", 40),
      ("Charlie", 55),
      ("Alibi", 18),
      ("Alibi", 28),
      ("Alibi", 38),
      ("Alibi", 48),
      ("Alibi", 58),
      ("Alibi", 68),
      ("Alibi", 78),
      ("Alibi", 88),
      ("Alibi", 98),
      ("Alibi", 108),
      ("Serzhan", 25),
      ("Aizharkyn", 38)
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.demo")

    spark
      .createDataFrame(usersList)
      .toDF("name", "age")
      .coalesce(1)
      .writeTo("rest.demo.users")
      .using("iceberg")
      .createOrReplace()

//    spark
//      .table("rest.demo.users")
//      .filter(col("age") >= 40)
//      .coalesce(1)
//      .writeTo("rest.demo.adult_users")
//      .using("iceberg")
//      .createOrReplace()

    spark.stop()
  }
}

