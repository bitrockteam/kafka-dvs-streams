package it.bitrock.dvs.streams.topologies

import java.util.Properties

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.config.AppConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream

object FlightReceivedStream {
  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val KeySerde: Serde[String]                         = kafkaStreamsOptions.stringKeySerde
    implicit val flightRawSerde: Serde[FlightRaw]                = kafkaStreamsOptions.flightRawSerde
    implicit val airportRawSerde: Serde[AirportRaw]              = kafkaStreamsOptions.airportRawSerde
    implicit val airlineRawSerde: Serde[AirlineRaw]              = kafkaStreamsOptions.airlineRawSerde
    implicit val airplaneRawSerde: Serde[AirplaneRaw]            = kafkaStreamsOptions.airplaneRawSerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived] = kafkaStreamsOptions.flightReceivedEventSerde

    val streamsBuilder   = new StreamsBuilder
    val flightRawStream  = streamsBuilder.stream[String, FlightRaw](config.kafka.topology.enhancedFlightRawTopic.name)
    val airportRawTable  = streamsBuilder.globalTable[String, AirportRaw](config.kafka.topology.airportRawTopic.name)
    val airlineRawTable  = streamsBuilder.globalTable[String, AirlineRaw](config.kafka.topology.airlineRawTopic.name)
    val airplaneRawTable = streamsBuilder.globalTable[String, AirplaneRaw](config.kafka.topology.airplaneRawTopic.name)

    val flightJoinAirport: KStream[String, FlightWithAllAirportInfo] =
      flightRawToAirportEnrichment(flightRawStream, airportRawTable)

    val flightAirportAirline: KStream[String, FlightWithAirline] =
      flightWithAirportToAirlineEnrichment(flightJoinAirport, airlineRawTable)

    val flightAirportAirlineAirplane: KStream[String, FlightReceived] =
      flightWithAirportAndAirlineToAirplaneEnrichment(flightAirportAirline, airplaneRawTable)

    flightAirportAirlineAirplane.to(config.kafka.topology.flightReceivedTopic.name)

    val props = streamProperties(config.kafka, config.kafka.topology.flightReceivedTopic.name)
    List((streamsBuilder.build(props), props))
  }

  private def flightRawToAirportEnrichment(
      flightRawStream: KStream[String, FlightRaw],
      airportRawTable: GlobalKTable[String, AirportRaw]
  ): KStream[String, FlightWithAllAirportInfo] =
    flightRawStream
      .join(airportRawTable)(
        (_, v) => v.departure.iataCode,
        flightRaw2FlightWithDepartureAirportInfo
      )
      .join(airportRawTable)(
        (_, v) => v.arrivalAirportCode,
        flightWithDepartureAirportInfo2FlightWithAllAirportInfo
      )

  private def flightWithAirportToAirlineEnrichment(
      flightWithAllAirportStream: KStream[String, FlightWithAllAirportInfo],
      airlineRawTable: GlobalKTable[String, AirlineRaw]
  ): KStream[String, FlightWithAirline] =
    flightWithAllAirportStream
      .join(airlineRawTable)(
        (_, v) => v.airlineCode,
        flightWithAllAirportInfo2FlightWithAirline
      )

  private def flightWithAirportAndAirlineToAirplaneEnrichment(
      flightWithAirline: KStream[String, FlightWithAirline],
      airplaneRawTable: GlobalKTable[String, AirplaneRaw]
  ): KStream[String, FlightReceived] =
    flightWithAirline
      .leftJoin(airplaneRawTable)(
        (_, v) => v.airplaneRegNumber,
        flightWithAirline2FlightReceived
      )

  private def flightRaw2FlightWithDepartureAirportInfo(
      flightRaw: FlightRaw,
      airportRaw: AirportRaw
  ): FlightWithDepartureAirportInfo =
    FlightWithDepartureAirportInfo(
      flightRaw.flight.iataNumber,
      flightRaw.flight.icaoNumber,
      GeographyInfo(
        flightRaw.geography.latitude,
        flightRaw.geography.longitude,
        flightRaw.geography.altitude,
        flightRaw.geography.direction
      ),
      flightRaw.speed.horizontal,
      AirportInfo(
        airportRaw.iataCode,
        airportRaw.name,
        airportRaw.latitude,
        airportRaw.longitude,
        airportRaw.countryName,
        airportRaw.countryIsoCode2,
        airportRaw.timezone,
        airportRaw.gmt
      ),
      flightRaw.arrival.iataCode,
      flightRaw.airline.icaoCode,
      flightRaw.aircraft.regNumber,
      flightRaw.status,
      flightRaw.system.updated
    )

  private def flightWithDepartureAirportInfo2FlightWithAllAirportInfo(
      flightWithDepartureAirportInfo: FlightWithDepartureAirportInfo,
      airportRaw: AirportRaw
  ): FlightWithAllAirportInfo =
    FlightWithAllAirportInfo(
      flightWithDepartureAirportInfo.iataNumber,
      flightWithDepartureAirportInfo.icaoNumber,
      flightWithDepartureAirportInfo.geography,
      flightWithDepartureAirportInfo.speed,
      flightWithDepartureAirportInfo.departureAirport,
      AirportInfo(
        airportRaw.iataCode,
        airportRaw.name,
        airportRaw.latitude,
        airportRaw.longitude,
        airportRaw.countryName,
        airportRaw.countryIsoCode2,
        airportRaw.timezone,
        airportRaw.gmt
      ),
      flightWithDepartureAirportInfo.airlineCode,
      flightWithDepartureAirportInfo.airplaneRegNumber,
      flightWithDepartureAirportInfo.status,
      flightWithDepartureAirportInfo.updated
    )

  private def flightWithAllAirportInfo2FlightWithAirline(
      flightWithAllAirportInfo: FlightWithAllAirportInfo,
      airlineRaw: AirlineRaw
  ): FlightWithAirline =
    FlightWithAirline(
      flightWithAllAirportInfo.iataNumber,
      flightWithAllAirportInfo.icaoNumber,
      flightWithAllAirportInfo.geography,
      flightWithAllAirportInfo.speed,
      flightWithAllAirportInfo.departureAirport,
      flightWithAllAirportInfo.arrivalAirport,
      AirlineInfo(airlineRaw.icaoCode, airlineRaw.name, airlineRaw.size),
      flightWithAllAirportInfo.airplaneRegNumber,
      flightWithAllAirportInfo.status,
      flightWithAllAirportInfo.updated
    )

  private def flightWithAirline2FlightReceived(flightWithAirline: FlightWithAirline, airplaneRaw: AirplaneRaw): FlightReceived =
    FlightReceived(
      flightWithAirline.iataNumber,
      flightWithAirline.icaoNumber,
      flightWithAirline.geography,
      flightWithAirline.speed,
      flightWithAirline.departureAirport,
      flightWithAirline.arrivalAirport,
      flightWithAirline.airline,
      airplaneInfoOrDefault(airplaneRaw),
      flightWithAirline.status,
      flightWithAirline.updated
    )

  private def airplaneInfoOrDefault(airplaneRaw: AirplaneRaw): AirplaneInfo =
    Option(airplaneRaw)
      .map(airplane => AirplaneInfo(airplane.registrationNumber, airplane.productionLine, airplane.modelCode))
      .getOrElse(AirplaneInfo("N/A", "N/A", "N/A"))
}
