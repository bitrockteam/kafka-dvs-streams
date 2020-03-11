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
  val defaultMissingValue = "N/A"

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val keySerde: Serde[String]                         = kafkaStreamsOptions.stringKeySerde
    implicit val flightRawSerde: Serde[FlightRaw]                = kafkaStreamsOptions.flightRawSerde
    implicit val airportRawSerde: Serde[AirportRaw]              = kafkaStreamsOptions.airportRawSerde
    implicit val airlineRawSerde: Serde[AirlineRaw]              = kafkaStreamsOptions.airlineRawSerde
    implicit val airplaneRawSerde: Serde[AirplaneRaw]            = kafkaStreamsOptions.airplaneRawSerde
    implicit val cityRawSerde: Serde[CityRaw]                    = kafkaStreamsOptions.cityRawSerde
    implicit val airportInfoSerde: Serde[AirportInfo]            = kafkaStreamsOptions.airportInfoSerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived] = kafkaStreamsOptions.flightReceivedEventSerde

    val streamsBuilder    = new StreamsBuilder
    val flightRawStream   = streamsBuilder.stream[String, FlightRaw](config.kafka.topology.enhancedFlightRawTopic.name)
    val airportRawStream  = streamsBuilder.stream[String, AirportRaw](config.kafka.topology.airportRawTopic.name)
    val airplaneRawStream = streamsBuilder.stream[String, AirplaneRaw](config.kafka.topology.airplaneRawTopic.name)
    val airlineRawTable   = streamsBuilder.globalTable[String, AirlineRaw](config.kafka.topology.airlineRawTopic.name)
    val cityRawTable      = streamsBuilder.globalTable[String, CityRaw](config.kafka.topology.cityRawTopic.name)

    airportRawStream
      .leftJoin(cityRawTable)((_, ar) => ar.cityIataCode, airportRaw2AirportInfo)
      .to(config.kafka.topology.airportInfoTopic.name)

    val airportInfoTable: GlobalKTable[String, AirportInfo] =
      streamsBuilder.globalTable[String, AirportInfo](config.kafka.topology.airportInfoTopic.name)

    airplaneRawStream
      .to(config.kafka.topology.airplaneRegistrationNumberRawTopic.name)

    airplaneRawStream
      .selectKey((_, v) => v.iataCode)
      .to(config.kafka.topology.airplaneIataCodeRawTopic.name)

    val airplaneRegNumberRawTable =
      streamsBuilder.globalTable[String, AirplaneRaw](config.kafka.topology.airplaneRegistrationNumberRawTopic.name)
    val airplaneIataCodeRawTable =
      streamsBuilder.globalTable[String, AirplaneRaw](config.kafka.topology.airplaneIataCodeRawTopic.name)

    val flightJoinAirport: KStream[String, FlightWithAllAirportInfo] =
      flightRawToAirportEnrichment(flightRawStream, airportInfoTable)

    val flightAirportAirline: KStream[String, FlightWithAirline] =
      flightWithAirportToAirlineEnrichment(flightJoinAirport, airlineRawTable)

    val flightAirportAirlineAirplane: KStream[String, FlightReceived] =
      flightWithAirportAndAirlineToAirplaneEnrichment(flightAirportAirline, airplaneRegNumberRawTable, airplaneIataCodeRawTable)

    flightAirportAirlineAirplane.to(config.kafka.topology.flightReceivedTopic.name)

    val props = streamProperties(config.kafka, config.kafka.topology.flightReceivedTopic.name)
    List((streamsBuilder.build(props), props))
  }

  private def airportRaw2AirportInfo(airportRaw: AirportRaw, cityRaw: CityRaw): AirportInfo = AirportInfo(
    airportRaw.iataCode,
    airportRaw.name,
    airportRaw.latitude,
    airportRaw.longitude,
    airportRaw.countryName,
    airportRaw.countryIsoCode2,
    airportRaw.timezone,
    airportRaw.gmt,
    Option(cityRaw).map(_.name).getOrElse(defaultMissingValue)
  )

  private def flightRawToAirportEnrichment(
      flightRawStream: KStream[String, FlightRaw],
      airportInfoTable: GlobalKTable[String, AirportInfo]
  ): KStream[String, FlightWithAllAirportInfo] =
    flightRawStream
      .join(airportInfoTable)(
        (_, v) => v.departure.iataCode,
        flightRaw2FlightWithDepartureAirportInfo
      )
      .join(airportInfoTable)(
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
      airplaneRegNumberRawTable: GlobalKTable[String, AirplaneRaw],
      airplaneIataCodeRawTable: GlobalKTable[String, AirplaneRaw]
  ): KStream[String, FlightReceived] =
    flightWithAirline
      .leftJoin(airplaneRegNumberRawTable)(
        (_, v) => v.airplaneRegNumber,
        Tuple2.apply
      )
      .leftJoin(airplaneIataCodeRawTable)(
        (_, v) => v._1.iataNumber, {
          case ((flight, airplane), otherAirplane) =>
            flightWithAirline2FlightReceived(flight, Option(airplane).getOrElse(otherAirplane))
        }
      )

  private def flightRaw2FlightWithDepartureAirportInfo(
      flightRaw: FlightRaw,
      airportInfo: AirportInfo
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
      airportInfo,
      flightRaw.arrival.iataCode,
      flightRaw.airline.icaoCode,
      flightRaw.aircraft.regNumber,
      flightRaw.status,
      flightRaw.system.updated
    )

  private def flightWithDepartureAirportInfo2FlightWithAllAirportInfo(
      flightWithDepartureAirportInfo: FlightWithDepartureAirportInfo,
      airportInfo: AirportInfo
  ): FlightWithAllAirportInfo =
    FlightWithAllAirportInfo(
      flightWithDepartureAirportInfo.iataNumber,
      flightWithDepartureAirportInfo.icaoNumber,
      flightWithDepartureAirportInfo.geography,
      flightWithDepartureAirportInfo.speed,
      flightWithDepartureAirportInfo.departureAirport,
      airportInfo,
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
      .getOrElse(AirplaneInfo(defaultMissingValue, defaultMissingValue, defaultMissingValue))
}
