package com.kuze.bigdata.study.serializer

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord

class AvroDeserializer extends AbstractKafkaAvroDeserializer {
  def this(client: SchemaRegistryClient) {
    this()
    // TODO: configure the deserializer for authentication
    this.schemaRegistry = client
  }

  override def deserialize(bytes: Array[Byte]): String = {
    val value = super.deserialize(bytes)
    value match {
      case str: String =>
        str
      case _ =>
        val genericRecord = value.asInstanceOf[GenericRecord]
        genericRecord.toString
    }
  }
}
