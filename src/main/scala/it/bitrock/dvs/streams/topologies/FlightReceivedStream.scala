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
import it.bitrock.dvs.streams.mapper.FlightReceivedMapper._

object FlightReceivedStream {

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val KeySerde: Serde[String]                         = kafkaStreamsOptions.stringKeySerde
    implicit val flightRawSerde: Serde[FlightRaw]                = kafkaStreamsOptions.flightRawSerde
    implicit val airportRawSerde: Serde[AirportRaw]              = kafkaStreamsOptions.airportRawSerde
    implicit val airlineRawSerde: Serde[AirlineRaw]              = kafkaStreamsOptions.airlineRawSerde
    implicit val airplaneRawSerde: Serde[AirplaneRaw]            = kafkaStreamsOptions.airplaneRawSerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived] = kafkaStreamsOptions.flightReceivedEventSerde

    val streamsBuilder   = new StreamsBuilder
    val flightRawStream  = streamsBuilder.stream[String, FlightRaw](config.kafka.topology.flightRawTopic.name)
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
        (flightRaw, airportRaw) => flightRaw.toFlightWithDepartureAirportInfo(airportRaw)
      )
      .join(airportRawTable)(
        (_, v) => v.codeAirportArrival,
        (flightReceivedOnlyDeparture, airportRaw) => flightReceivedOnlyDeparture.toFlightWithAllAirportInfo(airportRaw)
      )

  private def flightWithAirportToAirlineEnrichment(
      flightWithAllAirportStream: KStream[String, FlightWithAllAirportInfo],
      airlineRawTable: GlobalKTable[String, AirlineRaw]
  ): KStream[String, FlightWithAirline] =
    flightWithAllAirportStream
      .join(airlineRawTable)(
        (_, v) => v.airlineCode,
        (flightAndAirport, airlineRaw) => flightAndAirport.toFlightWithAirline(airlineRaw)
      )

  private def flightWithAirportAndAirlineToAirplaneEnrichment(
      flightWithAirline: KStream[String, FlightWithAirline],
      airplaneRawTable: GlobalKTable[String, AirplaneRaw]
  ): KStream[String, FlightReceived] =
    flightWithAirline
      .leftJoin(airplaneRawTable)(
        (_, v) => v.airplaneRegNumber,
        (flightAndAirline, airplaneRaw) => flightAndAirline.toFlightReceived(airplaneRaw)
      )

}
