package com.example.scala

import org.apache.spark.sql.functions.col

object IcebergRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef("IcebergRead")

    spark
      .table("rest.medallion.silver_hub_order")
      .filter("order_date >= '2025-01-01' AND order_date < '2025-02-01'")
      .show()

    spark.stop()
  }
}

