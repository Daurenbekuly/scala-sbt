package com.example.scala

import org.apache.spark.sql.functions.col

object IcebergRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef("IcebergRead")

    spark
      .table("rest.demo.users_salted")
      .filter(col("name") === "Alibi")
      .orderBy(col("age").desc)
      .show()

    spark.stop()
  }
}

