package com.example.scala

import org.apache.spark.sql.functions.{broadcast, col}

object BroadcastJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef()

    val users = spark.table("rest.demo.users")

    import spark.implicits._
    val segments = Seq(
      (0, 29, "young"),
      (30, 44, "adult"),
      (45, 200, "senior")
    ).toDF("age_min", "age_max", "segment")

    // Broadcast join on age between ranges
    val joined =
      users
        .join(
          broadcast(segments),
          users("age") >= segments("age_min") && users("age") <= segments("age_max"),
          joinType = "left"
        )

    // Persist result into a new Iceberg table
    joined
      .writeTo("rest.demo.users_segmented")
      .using("iceberg")
      .createOrReplace()

    spark.stop()
  }
}

