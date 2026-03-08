package com.example.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object IcebergWriteMerge {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("IcebergWrite")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.rest.type", "rest")
      .config("spark.sql.catalog.rest.uri", "http://localhost:8181")
      .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.catalog.rest.warehouse", "s3://warehouse/")
      .config("spark.sql.catalog.rest.client.region", "us-east-1")
      .config("spark.sql.catalog.rest.s3.endpoint", "http://localhost:9000")
      .config("spark.sql.catalog.rest.s3.path-style-access", "true")
      .config("spark.sql.catalog.rest.s3.access-key-id", "admin")
      .config("spark.sql.catalog.rest.s3.secret-access-key", "password")
      .getOrCreate()

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

    val df1 = spark.createDataFrame(usersList1).toDF("name", "age")
    df1
      .coalesce(1)
      .writeTo("rest.demo.merge_users")
      .using("iceberg")
      .createOrReplace()

    val df2 = spark.createDataFrame(usersList2).toDF("name", "age")
    df2.createOrReplaceTempView("updates")

    spark.sql("""
      MERGE INTO rest.demo.merge_users t
      USING updates s
      ON t.name = s.name
      WHEN MATCHED THEN UPDATE SET t.age = s.age
      WHEN NOT MATCHED THEN INSERT (name, age) VALUES (s.name, s.age)
    """)

    spark.stop()
  }
}

