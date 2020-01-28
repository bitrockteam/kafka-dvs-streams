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
    implicit val KeySerde: Serde[String]                         = kafkaStreamsOptions.keySerde
    implicit val flightRawSerde: Serde[FlightRaw]                = kafkaStreamsOptions.flightRawSerde
    implicit val airportRawSerde: Serde[AirportRaw]              = kafkaStreamsOptions.airportRawSerde
    implicit val airlineRawSerde: Serde[AirlineRaw]              = kafkaStreamsOptions.airlineRawSerde
    implicit val airplaneRawSerde: Serde[AirplaneRaw]            = kafkaStreamsOptions.airplaneRawSerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived] = kafkaStreamsOptions.flightReceivedEventSerde

    val streamsBuilder   = new StreamsBuilder
    val flightRawStream  = streamsBuilder.stream[String, FlightRaw](config.kafka.topology.flightRawTopic)
    val airportRawTable  = streamsBuilder.globalTable[String, AirportRaw](config.kafka.topology.airportRawTopic)
    val airlineRawTable  = streamsBuilder.globalTable[String, AirlineRaw](config.kafka.topology.airlineRawTopic)
    val airplaneRawTable = streamsBuilder.globalTable[String, AirplaneRaw](config.kafka.topology.airplaneRawTopic)

    val flightJoinAirport: KStream[String, FlightWithAllAirportInfo] =
      flightRawToAirportEnrichment(flightRawStream, airportRawTable)

    val flightAirportAirline: KStream[String, FlightWithAirline] =
      flightWithAirportToAirlineEnrichment(flightJoinAirport, airlineRawTable)

    val flightAirportAirlineAirplane: KStream[String, FlightReceived] =
      flightWithAirportAndAirlineToAirplaneEnrichment(flightAirportAirline, airplaneRawTable)

    flightAirportAirlineAirplane.to(config.kafka.topology.flightReceivedTopic)

    val props = streamProperties(config.kafka, config.kafka.topology.flightReceivedTopic)
    List((streamsBuilder.build(props), props))

  }

  private def flightRawToAirportEnrichment(
      flightRawStream: KStream[String, FlightRaw],
      airportRawTable: GlobalKTable[String, AirportRaw]
  ): KStream[String, FlightWithAllAirportInfo] =
    flightRawStream
      .join(airportRawTable)(
        (_, v) => v.departure.iataCode,
        (flightRaw, airportRaw) =>
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
              airportRaw.codeIataAirport,
              airportRaw.nameAirport,
              airportRaw.nameCountry,
              airportRaw.codeIso2Country,
              airportRaw.timezone,
              airportRaw.gmt
            ),
            flightRaw.arrival.iataCode,
            flightRaw.airline.icaoCode,
            flightRaw.aircraft.regNumber,
            flightRaw.status,
            flightRaw.system.updated
          )
      )
      .join(airportRawTable)(
        (_, v) => v.codeAirportArrival,
        (flightReceivedOnlyDeparture, airportRaw) =>
          FlightWithAllAirportInfo(
            flightReceivedOnlyDeparture.iataNumber,
            flightReceivedOnlyDeparture.icaoNumber,
            flightReceivedOnlyDeparture.geography,
            flightReceivedOnlyDeparture.speed,
            flightReceivedOnlyDeparture.airportDeparture,
            AirportInfo(
              airportRaw.codeIataAirport,
              airportRaw.nameAirport,
              airportRaw.nameCountry,
              airportRaw.codeIso2Country,
              airportRaw.timezone,
              airportRaw.gmt
            ),
            flightReceivedOnlyDeparture.airlineCode,
            flightReceivedOnlyDeparture.airplaneRegNumber,
            flightReceivedOnlyDeparture.status,
            flightReceivedOnlyDeparture.updated
          )
      )

  private def flightWithAirportToAirlineEnrichment(
      flightWithAllAirportStream: KStream[String, FlightWithAllAirportInfo],
      airlineRawTable: GlobalKTable[String, AirlineRaw]
  ): KStream[String, FlightWithAirline] =
    flightWithAllAirportStream
      .join(airlineRawTable)(
        (_, v) => v.airlineCode,
        (flightAndAirport, airlineRaw) =>
          FlightWithAirline(
            flightAndAirport.iataNumber,
            flightAndAirport.icaoNumber,
            flightAndAirport.geography,
            flightAndAirport.speed,
            flightAndAirport.airportDeparture,
            flightAndAirport.airportArrival,
            AirlineInfo(airlineRaw.codeIcaoAirline, airlineRaw.nameAirline, airlineRaw.sizeAirline),
            flightAndAirport.airplaneRegNumber,
            flightAndAirport.status,
            flightAndAirport.updated
          )
      )

  private def flightWithAirportAndAirlineToAirplaneEnrichment(
      flightWithAirline: KStream[String, FlightWithAirline],
      airplaneRawTable: GlobalKTable[String, AirplaneRaw]
  ): KStream[String, FlightReceived] =
    flightWithAirline
      .leftJoin(airplaneRawTable)(
        (_, v) => v.airplaneRegNumber,
        (flightAndAirline, airplaneRaw) =>
          FlightReceived(
            flightAndAirline.iataNumber,
            flightAndAirline.icaoNumber,
            flightAndAirline.geography,
            flightAndAirline.speed,
            flightAndAirline.airportDeparture,
            flightAndAirline.airportArrival,
            flightAndAirline.airline,
            airplaneInfoOrDefault(airplaneRaw),
            flightAndAirline.status,
            flightAndAirline.updated
          )
      )

  private def airplaneInfoOrDefault(airplaneRaw: AirplaneRaw): AirplaneInfo =
    Option(airplaneRaw)
      .map(airplane => AirplaneInfo(airplane.numberRegistration, airplane.productionLine, airplane.modelCode))
      .getOrElse(AirplaneInfo("N/A", "N/A", "N/A"))

}
