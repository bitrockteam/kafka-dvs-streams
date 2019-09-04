package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model._
import org.apache.kafka.common.serialization.Serde

final case class KafkaStreamsOptions(
    keySerde: Serde[String],
    flightRawSerde: Serde[FlightRaw],
    airportRawSerde: Serde[AirportRaw],
    airlineRawSerde: Serde[AirlineRaw],
    cityRawSerde: Serde[CityRaw],
    //output
    flightReceivedSerde: Serde[FlightReceived]
)
