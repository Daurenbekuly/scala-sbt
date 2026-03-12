package com.example.scala

object SparkSession {

  private val DefaultRestUri = "http://localhost:8181"
  private val DefaultS3Endpoint = "http://localhost:9000"
  private val DefaultMaster = "local[*]"

  def getOrCreateDef(appName: String): org.apache.spark.sql.SparkSession = synchronized {
    val conf = new org.apache.spark.SparkConf()

    val master = conf.get("spark.master", DefaultMaster)
    val restUri = conf.get("spark.sql.catalog.rest.uri", DefaultRestUri)
    val s3Endpoint = conf.get("spark.sql.catalog.rest.s3.endpoint", DefaultS3Endpoint)

    org.apache.spark.sql.SparkSession
      .builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.rest.type", "rest")
      .config("spark.sql.catalog.rest.uri", restUri)
      .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.catalog.rest.warehouse", "s3://warehouse/")
      .config("spark.sql.catalog.rest.client.region", "us-east-1")
      .config("spark.sql.catalog.rest.s3.endpoint", s3Endpoint)
      .config("spark.sql.catalog.rest.s3.path-style-access", "true")
      .config("spark.sql.catalog.rest.s3.access-key-id",
        conf.get("spark.sql.catalog.rest.s3.access-key-id", "admin"))
      .config("spark.sql.catalog.rest.s3.secret-access-key",
        conf.get("spark.sql.catalog.rest.s3.secret-access-key", "password"))
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
  }
}
