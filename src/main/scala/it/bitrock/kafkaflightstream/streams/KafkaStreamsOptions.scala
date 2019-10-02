package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model._
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
    flightReceivedEventSerde: Serde[FlightReceived],
    flightReceivedListEventSerde: Serde[FlightReceivedList],
    topAggregationKeySerde: Serde[Long],
    topArrivalAirportListEventSerde: Serde[TopArrivalAirportList],
    topDepartureAirportListEventSerde: Serde[TopDepartureAirportList],
    topAirportEventSerde: Serde[Airport],
    topSpeedListEventSerde: Serde[TopSpeedList],
    topSpeedFlightEventSerde: Serde[SpeedFlight],
    topAirlineListEventSerde: Serde[TopAirlineList],
    topAirlineEventSerde: Serde[Airline],
    countFlightEventSerde: Serde[CountFlight],
    countAirlineEventSerde: Serde[CountAirline]
)
