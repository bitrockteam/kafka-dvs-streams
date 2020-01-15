package it.bitrock.dvs.streams

import java.util.Properties

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import it.bitrock.dvs.streams.config.KafkaConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig

object StreamProps {

  final val UseSpecificAvroReader   = true
  final val AutoOffsetResetStrategy = OffsetResetStrategy.EARLIEST

  def streamProperties(config: KafkaConfig, applicationId: String): Properties = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, config.topology.threadsAmount.toString)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, config.topology.commitInterval.toMillis.toString)
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, config.topology.cacheMaxSizeBytes.toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.toString.toLowerCase)
    props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.topology.maxRequestSize.toString)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl.toString)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, UseSpecificAvroReader.toString)
    props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE)
    props
  }

}
