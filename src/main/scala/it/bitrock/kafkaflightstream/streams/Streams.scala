package it.bitrock.kafkaflightstream.streams

import java.util.Properties

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import it.bitrock.kafkaflightstream.model._
import it.bitrock.kafkaflightstream.streams.config.{AppConfig, KafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{StreamsConfig, Topology}

object Streams {

  // Whether or not to deserialize `SpecificRecord`s when possible
  final val UseSpecificAvroReader   = true
  final val AutoOffsetResetStrategy = OffsetResetStrategy.EARLIEST
  final val AllRecordsKey: String   = "all"

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): Topology = {
    implicit val KeySerde: Serde[String]          = kafkaStreamsOptions.keySerde
    implicit val flightRawSerde: Serde[FlightRaw] = kafkaStreamsOptions.flightRawSerde

    implicit val airportRawSerde: Serde[AirportRaw] = kafkaStreamsOptions.airportRawSerde

    implicit val airlineRawSerde: Serde[AirlineRaw] = kafkaStreamsOptions.airlineRawSerde

    implicit val cityRawSerde: Serde[CityRaw] = kafkaStreamsOptions.cityRawSerde

    implicit val rsvpReceivedSerde: Serde[FlightReceived] = kafkaStreamsOptions.flightReceivedSerde

    def buildFlightReceived(fligthtRawStream: KStream[String, FlightRaw], airportRawStreams: GlobalKTable[String, AirportRaw]): Unit = {
      flightRawToEnrichment(fligthtRawStream, airportRawStreams)
        .to(config.kafka.topology.flightReceivedTopic)
    }

    val streamsBuilder   = new StreamsBuilder
    val flightRawStream  = streamsBuilder.stream[String, FlightRaw](config.kafka.topology.flightRawTopic)
    val airportRawStream = streamsBuilder.globalTable[String, AirportRaw](config.kafka.topology.airportRawTopic)
    // val airlineRawStream  = streamsBuilder.globalTable[String, AirlineRaw](config.kafka.topology.airlineRawTopic)
    // val cityRawStream  = streamsBuilder.globalTable[String, CityRaw](config.kafka.topology.cityRawTopic)

    buildFlightReceived(flightRawStream, airportRawStream)

    streamsBuilder.build
  }

  def streamProperties(config: KafkaConfig): Properties = {
    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, config.topology.cacheMaxSizeBytes.toString)
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, config.topology.threadsAmount.toString)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, config.topology.commitInterval.toMillis.toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.toString.toLowerCase)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl.toString)
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, UseSpecificAvroReader.toString)

    props
  }

  def flightRawToEnrichment(
      flightRawStream: KStream[String, FlightRaw],
      airportRawTable: GlobalKTable[String, AirportRaw]
  ): KStream[String, FlightReceived] = {

    flightRawStream
      .join(airportRawTable)(
        (_, value) => value.departure.iataCode,
        (flight, airport) =>
          FlightReceived(
            GeographyEvent(flight.geography.latitude, flight.geography.longitude, flight.geography.altitude, flight.geography.direction),
            flight.speed.horizontal,
            airport.nameAirport,
            flight.arrival.iataCode,
            flight.status
          )
      )
      .join(airportRawTable)(
        (_, value2) => value2.nameAirportArrival, //iatacode arrivo
        (flightReceived, airport) => flightReceived.copy(nameAirportArrival = airport.nameAirport)
      )
  }
}
