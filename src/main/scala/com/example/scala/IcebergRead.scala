package com.example.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object IcebergRead {
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

    val df = spark.read.format("iceberg").load("rest.demo.merge_users")
    df.show()

    spark.stop()
  }
}

