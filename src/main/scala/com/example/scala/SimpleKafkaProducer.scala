package com.example.scala

import org.apache.spark.sql.functions._

object SimpleKafkaProducer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.getOrCreateDef();

    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    val df = spark.table("rest.demo.users")

    df
      .select(to_json(struct(col("name"), col("age"))).as("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", "my-topic")
      .save()
  }
}