package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Instant

import it.bitrock.dvs.model.avro.FlightRaw
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.{Grouped, KStream, Materialized}
import it.bitrock.dvs.streams.topologies.infer_flights.lib.ImplicitSerde._

object DeduplicateFlightRaw {
  implicit val wrapperSerde: Serde[FlightRawWrapper] = Serde[FlightRawWrapper]
  implicit val groupSerde: Grouped[String, FlightRawWrapper] = Grouped.`with`(keySerde, wrapperSerde)

  def apply(stream: KStream[String, FlightRaw]): KStream[String, FlightRaw] = {
    stream
      .mapValues(FlightRawWrapper(_, forward = true))
      .groupByKey
      .reduce((previous, last) => {
        if (last.compareTo(previous) > 0) {
          last
        } else {
          FlightRawWrapper(last.flightRaw, forward = false)
        }
      })(Materialized.`with`(keySerde, wrapperSerde))
      .filter((_, value) => value.forward)
      .mapValues(_.flightRaw)
      .toStream
  }

  private case class FlightRawWrapper(flightRaw: FlightRaw, forward: Boolean) extends Comparable[FlightRawWrapper] {
    def flightUpdated: Instant = flightRaw.system.updated

    override def compareTo(other: FlightRawWrapper): Int = flightUpdated.compareTo(other.flightUpdated)
  }
}
