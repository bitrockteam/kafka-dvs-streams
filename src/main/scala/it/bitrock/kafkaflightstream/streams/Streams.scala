package it.bitrock.kafkaflightstream.streams

import java.util.Properties

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import it.bitrock.kafkaflightstream.model._
import it.bitrock.kafkaflightstream.streams.config.{AppConfig, KafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, TimeWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{StreamsConfig, Topology}

object Streams {

  // Whether or not to deserialize `SpecificRecord`s when possible
  final val UseSpecificAvroReader   = true
  final val AutoOffsetResetStrategy = OffsetResetStrategy.EARLIEST
  final val AllRecordsKey: String   = "all"

  private final val europeanCountries = Set(
    "AL",
    "AD",
    "AT",
    "BE",
    "BY",
    "BA",
    "BG",
    "CY",
    "HR",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "GR",
    "IE",
    "IS",
    "IT",
    "XK",
    "LV",
    "LI",
    "LT",
    "LU",
    "MK",
    "MT",
    "MD",
    "MC",
    "ME",
    "NO",
    "NL",
    "PL",
    "PT",
    "GB",
    "CZ",
    "RO",
    "RU",
    "SM",
    "RS",
    "SK",
    "SI",
    "ES",
    "SE",
    "CH",
    "UA",
    "HU",
    "VA"
  )

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

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): Topology = {
    implicit val KeySerde: Serde[String]              = kafkaStreamsOptions.keySerde
    implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
    implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
    implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
    implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
    //implicit val cityRawSerde: Serde[CityRaw] = kafkaStreamsOptions.cityRawSerde

    //output topic
    implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent]         = kafkaStreamsOptions.flightEnrichedEventSerde
    implicit val topAggregationKeySerde: Serde[Long]                          = kafkaStreamsOptions.topAggregationKeySerde
    implicit val topArrivalAirportListSerde: Serde[TopArrivalAirportList]     = kafkaStreamsOptions.topArrivalAirportListEventSerde
    implicit val topDepartureAirportListSerde: Serde[TopDepartureAirportList] = kafkaStreamsOptions.topDepartureAirportListEventSerde
    implicit val topSpeedListSerde: Serde[TopSpeedList]                       = kafkaStreamsOptions.topSpeedListEventSerde
    implicit val topAirportSerde: Serde[Airport]                              = kafkaStreamsOptions.topAirportEventSerde
    implicit val topSpeedSerde: Serde[SpeedFlight]                            = kafkaStreamsOptions.topSpeedFlightEventSerde
    implicit val countFlightStatusSerde: Serde[CountFlightStatus]             = kafkaStreamsOptions.countFlightStatusEventSerde

    def buildFlightReceived(
        fligthtRawStream: KStream[String, FlightRaw],
        airportRawTable: GlobalKTable[String, AirportRaw],
        airlineRawTable: GlobalKTable[String, AirlineRaw],
        airplaneRawTable: GlobalKTable[String, AirplaneRaw]
    ): KStream[String, FlightEnrichedEvent] = {

      val flightJoinAirport: KStream[String, FlightWithAllAirportInfo] = flightRawToAirportEnrichment(fligthtRawStream, airportRawTable)

      val flightAirportAirline: KStream[String, FlightWithAirline] =
        flightWithAirportToAirlineEnrichment(flightJoinAirport, airlineRawTable)

      val flightAirportAirlineAirplane: KStream[String, FlightEnrichedEvent] =
        flightWithAirportAndAirlineToAirplaneEnrichment(flightAirportAirline, airplaneRawTable)

      flightAirportAirlineAirplane.to(config.kafka.topology.flightReceivedTopic)
      flightAirportAirlineAirplane
    }

    def buildTopArrival(flightEnriched: KStream[String, FlightEnrichedEvent]): Unit = {
      val topArrivalAirportAggregator = new TopArrivalAirportAggregator(config.topElementsAmount)

      flightEnriched
        .groupBy((_, value) => value.airportArrival.codeAirport)
        .windowedBy(TimeWindows.of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize)))
        .count
        .groupBy((k, v) => (k.window.start.toString, Airport(k.key, v)))
        .aggregate(topArrivalAirportAggregator.initializer)(topArrivalAirportAggregator.adder, topArrivalAirportAggregator.subtractor)
        .toStream
        .to(config.kafka.topology.topArrivalAirportTopic)
    }

    def buildTopDeparture(flightEnriched: KStream[String, FlightEnrichedEvent]): Unit = {
      val topDepartureAirportAggregator = new TopDepartureAirportAggregator(config.topElementsAmount)

      flightEnriched
        .groupBy((_, value) => value.airportDeparture.codeAirport)
        .windowedBy(TimeWindows.of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize)))
        .count
        .groupBy((k, v) => (k.window.start.toString, Airport(k.key, v)))
        .aggregate(topDepartureAirportAggregator.initializer)(topDepartureAirportAggregator.adder, topDepartureAirportAggregator.subtractor)
        .toStream
        .to(config.kafka.topology.topDepartureAirportTopic)
    }

    def buildTopFlightSpeed(flightEnriched: KStream[String, FlightEnrichedEvent]): Unit = {
      val topSpeedFlightAggregator = new TopSpeedFlightAggregator(config.topElementsAmount)

      flightEnriched.groupByKey
        .windowedBy(TimeWindows.of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize)))
        .reduce(lastUpdatedFlight)
        .groupBy((k, v) => (k.window.start.toString, SpeedFlight(k.key, v.speed)))
        .aggregate(topSpeedFlightAggregator.initializer)(topSpeedFlightAggregator.adder, topSpeedFlightAggregator.subtractor)
        .toStream
        .to(config.kafka.topology.topSpeedTopic)
    }

    def buildTotalFlights(flightEnriched: KStream[String, FlightEnrichedEvent]): Unit = {
      val countFlightAggregator = new CountFlightAggregator()

      flightEnriched.groupByKey
        .reduce(lastUpdatedFlight)
        .groupBy((_, v) => (v.status, v))
        .aggregate(countFlightAggregator.initializer)(countFlightAggregator.adder, countFlightAggregator.subtractor)
        .toStream
        .to(config.kafka.topology.totalFlightTopic)
    }

    val streamsBuilder   = new StreamsBuilder
    val flightRawStream  = streamsBuilder.stream[String, FlightRaw](config.kafka.topology.flightRawTopic)
    val airportRawTable  = streamsBuilder.globalTable[String, AirportRaw](config.kafka.topology.airportRawTopic)
    val airlineRawTable  = streamsBuilder.globalTable[String, AirlineRaw](config.kafka.topology.airlineRawTopic)
    val airplaneRawTable = streamsBuilder.globalTable[String, AirplaneRaw](config.kafka.topology.airplaneRawTopic)
    //val cityRawStream  = streamsBuilder.globalTable[String, CityRaw](config.kafka.topology.cityRawTopic)

    val flightReceivedStream = buildFlightReceived(flightRawStream, airportRawTable, airlineRawTable, airplaneRawTable)
    buildTopArrival(flightReceivedStream)
    buildTopDeparture(flightReceivedStream)
    buildTopFlightSpeed(flightReceivedStream)
    buildTotalFlights(flightReceivedStream)

    streamsBuilder.build
  }

  private def flightRawToAirportEnrichment(
      flightRawStream: KStream[String, FlightRaw],
      airportRawTable: GlobalKTable[String, AirportRaw]
  ): KStream[String, FlightWithAllAirportInfo] = {

    flightRawStream
      .join(airportRawTable)(
        (_, value) => value.departure.iataCode,
        (flightRaw, airportRaw) =>
          FlightWithDepartureAirportInfo(
            flightRaw.flight.iataNumber,
            GeographyInfo(
              flightRaw.geography.latitude,
              flightRaw.geography.longitude,
              flightRaw.geography.altitude,
              flightRaw.geography.direction
            ),
            flightRaw.speed.horizontal,
            AirportInfo(airportRaw.codeIataAirport, airportRaw.nameAirport, airportRaw.nameCountry, airportRaw.codeIso2Country),
            flightRaw.arrival.iataCode,
            flightRaw.airline.icaoCode,
            flightRaw.aircraft.regNumber,
            flightRaw.status,
            flightRaw.system.updated
          )
      )
      .join(airportRawTable)(
        (_, value2) => value2.codeAirportArrival,
        (flightReceivedOnlyDeparture, airportRaw) =>
          FlightWithAllAirportInfo(
            flightReceivedOnlyDeparture.iataNumber,
            flightReceivedOnlyDeparture.geography,
            flightReceivedOnlyDeparture.speed,
            flightReceivedOnlyDeparture.airportDeparture,
            AirportInfo(airportRaw.codeIataAirport, airportRaw.nameAirport, airportRaw.nameCountry, airportRaw.codeIso2Country),
            flightReceivedOnlyDeparture.airlineCode,
            flightReceivedOnlyDeparture.airplaneRegNumber,
            flightReceivedOnlyDeparture.status,
            flightReceivedOnlyDeparture.updated
          )
      )
      .filter(
        (_, value) =>
          europeanCountries.contains(value.airportDeparture.codeIso2Country) &&
            europeanCountries.contains(value.airportArrival.codeIso2Country)
      )
  }

  private def flightWithAirportToAirlineEnrichment(
      flightWithAllAirportStream: KStream[String, FlightWithAllAirportInfo],
      airlineRawTable: GlobalKTable[String, AirlineRaw]
  ): KStream[String, FlightWithAirline] = {

    flightWithAllAirportStream
      .join(airlineRawTable)(
        (_, value) => value.airlineCode,
        (flightAndAirport, airlineRaw) =>
          FlightWithAirline(
            flightAndAirport.iataNumber,
            flightAndAirport.geography,
            flightAndAirport.speed,
            flightAndAirport.airportDeparture,
            flightAndAirport.airportArrival,
            AirlineInfo(airlineRaw.nameAirline, airlineRaw.sizeAirline),
            flightAndAirport.airplaneRegNumber,
            flightAndAirport.status,
            flightAndAirport.updated
          )
      )
  }

  private def flightWithAirportAndAirlineToAirplaneEnrichment(
      flightWithAirline: KStream[String, FlightWithAirline],
      airplaneRawTable: GlobalKTable[String, AirplaneRaw]
  ): KStream[String, FlightEnrichedEvent] = {
    flightWithAirline
      .leftJoin(airplaneRawTable)(
        (_, value) => value.airplaneRegNumber,
        (flightAndAirline, airplaneRaw) =>
          FlightEnrichedEvent(
            flightAndAirline.iataNumber,
            flightAndAirline.geography,
            flightAndAirline.speed,
            flightAndAirline.airportDeparture,
            flightAndAirline.airportArrival,
            flightAndAirline.airline,
            Option(airplaneRaw).map(airplaneRaw => AirplaneInfo(airplaneRaw.productionLine, airplaneRaw.modelCode)),
            flightAndAirline.status,
            flightAndAirline.updated
          )
      )
  }

  private val lastUpdatedFlight: (FlightEnrichedEvent, FlightEnrichedEvent) => FlightEnrichedEvent =
    (v1, v2) => if (v1.updated.toLong > v2.updated.toLong) v1 else v2

}
