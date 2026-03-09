package com.example.scala

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SimpleKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SimpleKafkaConsumer")
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

    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "my-topic")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))

    val messages = df
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.demo")
    spark.sql("CREATE TABLE IF NOT EXISTS rest.demo.kafka_users (name STRING, age INT) USING iceberg")

    val query = messages
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "/tmp/kafka-consumer-checkpoint")
      .foreachBatch { (batchDF: Dataset[Row], _: Long) =>
        if (!batchDF.isEmpty) {
          val ws = Window
            .partitionBy(col("name"))
            .orderBy(col("age").desc)

          val dedup = batchDF
            .withColumn("rn", row_number().over(ws))
            .filter(col("rn") === 1)
            .drop("rn")

          dedup.createOrReplaceTempView("batch_updates")
          batchDF.sparkSession.sql(
            """
            MERGE INTO rest.demo.kafka_users t
            USING batch_updates s
            ON t.name = s.name
            WHEN MATCHED THEN UPDATE SET t.age = s.age
            WHEN NOT MATCHED THEN INSERT (name, age) VALUES (s.name, s.age)
          """)
        }
        ()
      }
      .start()

    query.awaitTermination()
  }
}