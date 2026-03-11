package com.example.scala

import org.apache.spark.sql.functions.{col, rand}

object Repartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef()

    val users = spark.table("rest.demo.users")

    val byName = users.repartition(col("name"))

    val numSalts = 8
    val salted =
      byName
        .withColumn("salt", (rand() * numSalts).cast("int"))
        .repartition(numSalts, col("name"), col("salt"))

    salted
      .writeTo("rest.demo.users_salted")
      .using("iceberg")
      .createOrReplace()

    spark.stop()
  }
}

