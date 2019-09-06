package it.bitrock.kafkaflightstream.streams

import java.util.Properties

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import it.bitrock.kafkaflightstream.model.{AirportInfo, _}
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
    implicit val KeySerde: Serde[String]              = kafkaStreamsOptions.keySerde
    implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
    implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
    implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
    implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
    //not used
    implicit val cityRawSerde: Serde[CityRaw] = kafkaStreamsOptions.cityRawSerde

    //for join trasformation
    implicit val flightWithDepartureAirportInfoSerde: Serde[FlightWithDepartureAirportInfo] =
      kafkaStreamsOptions.flightWithDepartureAirportInfo
    implicit val flightWithAllAirportSerde: Serde[FlightWithAllAirportInfo] = kafkaStreamsOptions.flightWithAllAirportInfo
    implicit val flightWithAirlineSerde: Serde[FlightWithAirline]           = kafkaStreamsOptions.flightWithAirline

    //output topic
    implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent] = kafkaStreamsOptions.flightEnrichedEventSerde

    def buildFlightReceived(
        fligthtRawStream: KStream[String, FlightRaw],
        airportRawTable: GlobalKTable[String, AirportRaw],
        airlineRawTable: GlobalKTable[String, AirlineRaw],
        airplaneRawTable: GlobalKTable[String, AirplaneRaw]
    ): Unit = {

      println("flightJoinAirport ->")
      val flightJoinAirport: KStream[String, FlightWithAllAirportInfo] = flightRawToAirportEnrichment(fligthtRawStream, airportRawTable)

      println("flightAirportAirline ->")
      val flightAirportAirline: KStream[String, FlightWithAirline] = flightWithAirportToAirlineEnrichment(flightJoinAirport, airlineRawTable)

      println("flightAirportAirlineAirplane ->")
      val flightAirportAirlineAirplane: KStream[String, FlightEnrichedEvent] = flightWithAirportAndAirlineToAirplaneEnrichment(flightAirportAirline, airplaneRawTable)

      println("sto scrivendo su topic -> ")
      flightAirportAirlineAirplane.to(config.kafka.topology.flightReceivedTopic)
    }

    val streamsBuilder   = new StreamsBuilder
    val flightRawStream  = streamsBuilder.stream[String, FlightRaw](config.kafka.topology.flightRawTopic)
    val airportRawTable  = streamsBuilder.globalTable[String, AirportRaw](config.kafka.topology.airportRawTopic)
    val airlineRawTable  = streamsBuilder.globalTable[String, AirlineRaw](config.kafka.topology.airlineRawTopic)
    val airplaneRawTable = streamsBuilder.globalTable[String, AirplaneRaw](config.kafka.topology.airplaneRawTopic)
    //not used
    // val cityRawStream  = streamsBuilder.globalTable[String, CityRaw](config.kafka.topology.cityRawTopic)

    buildFlightReceived(flightRawStream, airportRawTable, airlineRawTable, airplaneRawTable)

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

  def flightRawToAirportEnrichment(
      flightRawStream: KStream[String, FlightRaw],
      airportRawTable: GlobalKTable[String, AirportRaw]
  ): KStream[String, FlightWithAllAirportInfo] = {

    flightRawStream
      .join(airportRawTable)(
        (_, value) => value.departure.iataCode,
        (flight, airport) =>
          FlightWithDepartureAirportInfo(
            GeographyInfo(flight.geography.latitude, flight.geography.longitude, flight.geography.altitude, flight.geography.direction),
            flight.speed.horizontal,
            AirportInfo(airport.codeIataAirport, airport.nameAirport, airport.nameCountry, airport.codeIso2Country), //departure
            flight.arrival.iataCode,                                                                                 //Arrival
            flight.airline.iataCode,
            flight.aircraft.regNumber,
            flight.status
          )
      )
      .join(airportRawTable)(
        (_, value2) => value2.codeAirportArrival, //iatacode arrivo
        (flightReceivedOnlyDeparture, airport) =>
          FlightWithAllAirportInfo(
            flightReceivedOnlyDeparture.geography,
            flightReceivedOnlyDeparture.speed,
            flightReceivedOnlyDeparture.airportDeparture,
            AirportInfo(airport.codeIataAirport, airport.nameAirport, airport.nameCountry, airport.codeIso2Country),
            flightReceivedOnlyDeparture.airlineCode,
            flightReceivedOnlyDeparture.airplaneRegNumber,
            flightReceivedOnlyDeparture.status
          )
      )
  }

  def flightWithAirportToAirlineEnrichment(
      flightWithAllAirportStream: KStream[String, FlightWithAllAirportInfo],
      airlineRawTable: GlobalKTable[String, AirlineRaw]
  ): KStream[String, FlightWithAirline] = {

    flightWithAllAirportStream
      .join(airlineRawTable)(
        (_, value) => value.airlineCode,
        (flightAndAirport, airline) =>
          FlightWithAirline(
            flightAndAirport.geography,
            flightAndAirport.speed,
            flightAndAirport.airportDeparture,
            flightAndAirport.airportArrival,
            AirlineInfo(airline.nameAirline, airline.sizeAirline),
            flightAndAirport.airplaneRegNumber,
            flightAndAirport.status
          )
      )
  }

  def flightWithAirportAndAirlineToAirplaneEnrichment(
      flightWithAirline: KStream[String, FlightWithAirline],
      airplaneRawTable: GlobalKTable[String, AirplaneRaw]
  ): KStream[String, FlightEnrichedEvent] = {
    flightWithAirline
      .leftJoin(airplaneRawTable)(
        (_, value) => value.airplaneRegNumber,
        (flightAndAirline, airplaneTable) =>
          FlightEnrichedEvent(
            flightAndAirline.geography,
            flightAndAirline.speed,
            flightAndAirline.airportDeparture,
            flightAndAirline.airportArrival,
            flightAndAirline.airline,
            Option(airplaneTable).map(airplaneTable => AirplaneInfo(airplaneTable.productionLine, airplaneTable.modelCode)),
            flightAndAirline.status
          )
      )
  }
}
