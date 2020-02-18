package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Instant

import it.bitrock.dvs.model.avro.FlightRaw
import it.bitrock.dvs.streams.KafkaStreamsOptions
import org.apache.kafka.streams.scala.kstream.{Grouped, KStream, Materialized}
import it.bitrock.dvs.streams.config.KafkaConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}

object DeduplicateFlightRaw {
  val wrapperSerde: Serde[FlightRawWrapper] = Serdes.serdeFrom(classOf[FlightRawWrapper])

  def apply(stream: KStream[String, FlightRaw], kafkaStreamsOptions: KafkaStreamsOptions): KStream[String, FlightRaw] = {
    implicit val groupSerde: Grouped[String, FlightRawWrapper] = Grouped.`with`(kafkaStreamsOptions.stringKeySerde, wrapperSerde)
    stream
      .mapValues(FlightRawWrapper(_, forward = true))
      .groupByKey
      .reduce { (previous, last) =>
        if (last.compareTo(previous) > 0) {
          last
        } else {
          FlightRawWrapper(last.flightRaw, forward = false)
        }
      }(Materialized.`with`(kafkaStreamsOptions.stringKeySerde, wrapperSerde))
      .filter((_, value) => value.forward)
      .mapValues(_.flightRaw)
      .toStream
  }

  case class FlightRawWrapper(flightRaw: FlightRaw, forward: Boolean) extends Comparable[FlightRawWrapper] {
    def flightUpdated: Instant = flightRaw.system.updated

    override def compareTo(other: FlightRawWrapper): Int = flightUpdated.compareTo(other.flightUpdated)
  }
}
