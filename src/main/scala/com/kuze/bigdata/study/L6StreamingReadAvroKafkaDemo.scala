package com.kuze.bigdata.study

import java.util

import com.kuze.bigdata.study.serializer.AvroDeserializer
import com.xmfunny.turbine.serializer.AvroDeserializer
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object L6StreamingReadAvroKafkaDemo {

  private var schemaRegistryClient: SchemaRegistryClient = _

  private var kafkaAvroDeserializer: AvroDeserializer = _

  def lookupTopicSchema(topic: String, isKey: Boolean = false) = {
    schemaRegistryClient.getLatestSchemaMetadata(topic + (if (isKey) "-key" else "-value")).getSchema
  }

  def avroSchemaToSparkSchema(avroSchema: String) = {
    SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))
  }

  def main(args: Array[String]): Unit = {

    val topic = "dev-avro-user-topic001";
    val schemaRegistryUrl = "http://localhost:8081";
    val basePath = "file:///tmp/hudi_cow_table"

    val spark = SparkSession
      .builder
      .appName("test")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)
    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
      kafkaAvroDeserializer.deserialize(bytes)
    )

    val rawDf = spark
      .readStream
      .format("kafka")
      .option("kafka.isolation.level", "read_committed")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("group.id", "1")
      .load()

    val jsonDf = rawDf.select(
      // 'key.cast(DataTypes.StringType),  // string keys are simplest to use
      col("key").cast("string").as("key"),
      callUDF("deserialize", col("value")).as("value")
      // excluding topic, partition, offset, timestamp, etc
    )

    // Get the Avro schema for the topic from the Schema Registry and convert it into a Spark schema type
    val dfValueSchema = {
      val rawSchema = lookupTopicSchema(topic)
      avroSchemaToSparkSchema(rawSchema)
    }

    // Apply structured schema to JSON stream
    val parsedDf: DataFrame = jsonDf.select(

      col("key"),
      // values are JSONified Avro records
      from_json(col("value"), dfValueSchema.dataType).alias("value")
    ).select(
      "key",
      "value.*"
    )

    val query = parsedDf.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("6 seconds"))
      .format("console")
      .start()

    query.awaitTermination()

  }

  def getQuickstartWriteConfigs: util.Map[String, String] = {
    val demoConfigs: util.Map[String, String] = new util.HashMap[String, String]
    demoConfigs.put("hoodie.insert.shuffle.parallelism", "2")
    demoConfigs.put("hoodie.upsert.shuffle.parallelism", "2")
    demoConfigs
  }

}
