package it.bitrock.dvs.streams.topologies.infer_flights.lib

import it.bitrock.dvs.model.avro.FlightRaw
import it.bitrock.dvs.streams.topologies.infer_flights.model.FlightRawTs
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.Grouped

object ImplicitSerde {
  implicit val keySerde: Serde[String] = Serde[String]
  implicit val flightRawSerde: Serde[FlightRaw] = Serde[FlightRaw]
  implicit val flightRawGroupedSerde: Grouped[String, FlightRaw] = Grouped.`with`(keySerde, flightRawSerde)
  val flightRawTsSerde: Serde[FlightRawTs] = Serde[FlightRawTs]
}
