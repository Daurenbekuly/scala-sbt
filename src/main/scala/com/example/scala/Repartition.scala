package com.example.scala

import org.apache.spark.sql.functions.{col, rand}

object Repartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef()

    val numSalts = 8

    spark.table("rest.demo.users")
      .repartition(col("name"))
      .withColumn("salt", (rand() * numSalts).cast("int"))
      .repartition(numSalts, col("name"), col("salt"))
      .writeTo("rest.demo.users_salted")
      .using("iceberg")
      .createOrReplace()

    spark.stop()
  }
}

