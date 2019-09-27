package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model.{
  Airline,
  AirlineRaw,
  AirplaneRaw,
  Airport,
  AirportRaw,
  CityRaw,
  CountAirline,
  CountFlightStatus,
  FlightEnrichedEvent,
  FlightRaw,
  FlightReceivedList,
  FlightWithAirline,
  FlightWithAllAirportInfo,
  FlightWithDepartureAirportInfo,
  SpeedFlight,
  TopAirlineList,
  TopArrivalAirportList,
  TopDepartureAirportList,
  TopSpeedList
}
import org.apache.kafka.common.serialization.Serde

final case class KafkaStreamsOptions(
    keySerde: Serde[String],
    flightRawSerde: Serde[FlightRaw],
    airportRawSerde: Serde[AirportRaw],
    airlineRawSerde: Serde[AirlineRaw],
    cityRawSerde: Serde[CityRaw],
    airplaneRawSerde: Serde[AirplaneRaw],
    //output
    flightWithDepartureAirportInfo: Serde[FlightWithDepartureAirportInfo],
    flightWithAllAirportInfo: Serde[FlightWithAllAirportInfo],
    flightWithAirline: Serde[FlightWithAirline],
    //final output
    flightEnrichedEventSerde: Serde[FlightEnrichedEvent],
    flightReceivedListEventSerde: Serde[FlightReceivedList],
    topAggregationKeySerde: Serde[Long],
    topArrivalAirportListEventSerde: Serde[TopArrivalAirportList],
    topDepartureAirportListEventSerde: Serde[TopDepartureAirportList],
    topAirportEventSerde: Serde[Airport],
    topSpeedListEventSerde: Serde[TopSpeedList],
    topSpeedFlightEventSerde: Serde[SpeedFlight],
    topAirlineListEventSerde: Serde[TopAirlineList],
    topAirlineEventSerde: Serde[Airline],
    countFlightStatusEventSerde: Serde[CountFlightStatus],
    countAirlineEventSerde: Serde[CountAirline]
)
