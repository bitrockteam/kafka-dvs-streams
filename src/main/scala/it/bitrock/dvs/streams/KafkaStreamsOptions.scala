package it.bitrock.dvs.streams

import it.bitrock.dvs.model.avro._
import org.apache.kafka.common.serialization.Serde

final case class KafkaStreamsOptions(
    keySerde: Serde[String],
    flightRawSerde: Serde[FlightRaw],
    airportRawSerde: Serde[AirportRaw],
    airlineRawSerde: Serde[AirlineRaw],
    cityRawSerde: Serde[CityRaw],
    airplaneRawSerde: Serde[AirplaneRaw],
    flightWithDepartureAirportInfo: Serde[FlightWithDepartureAirportInfo],
    flightWithAllAirportInfo: Serde[FlightWithAllAirportInfo],
    flightWithAirline: Serde[FlightWithAirline],
    flightReceivedEventSerde: Serde[FlightReceived],
    flightReceivedListEventSerde: Serde[FlightReceivedList],
    topAggregationKeySerde: Serde[Long],
    topArrivalAirportListEventSerde: Serde[TopArrivalAirportList],
    topDepartureAirportListEventSerde: Serde[TopDepartureAirportList],
    topAirportEventSerde: Serde[TopAirport],
    topSpeedListEventSerde: Serde[TopSpeedList],
    topSpeedFlightEventSerde: Serde[TopSpeed],
    topAirlineListEventSerde: Serde[TopAirlineList],
    topAirlineEventSerde: Serde[TopAirline],
    countFlightEventSerde: Serde[CountFlight],
    countAirlineEventSerde: Serde[CountAirline],
    codeAirlineListEventSerde: Serde[CodeAirlineList],
    flightNumberListEventSerde: Serde[FlightNumberList],
    computationStatusSerde: Serde[ComputationStatus]
)
