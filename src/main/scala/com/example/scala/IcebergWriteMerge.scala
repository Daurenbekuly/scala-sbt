package com.example.scala

object IcebergWriteMerge {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef()

    val usersList1 = List(
      ("Alibi", 28),
      ("Serzhan", 25),
      ("Aizharkyn", 38)
    )

    val usersList2 = List(
      ("Alice", 35),
      ("Bob", 40),
      ("Charlie", 55)
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.demo")

    spark
      .createDataFrame(usersList1)
      .toDF("name", "age")
      .coalesce(1)
      .writeTo("rest.demo.merge_users")
      .using("iceberg")
      .createOrReplace()

    spark
      .createDataFrame(usersList2)
      .toDF("name", "age")
      .createOrReplaceTempView("updates")

    spark.sql(
      """
      MERGE INTO rest.demo.merge_users t
      USING updates s
      ON t.name = s.name
      WHEN MATCHED THEN UPDATE SET t.age = s.age
      WHEN NOT MATCHED THEN INSERT (name, age) VALUES (s.name, s.age)
    """)

    spark.stop()
  }
}

