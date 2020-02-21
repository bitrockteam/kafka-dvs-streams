package it.bitrock.dvs.streams.topologies.infer_flights

import it.bitrock.dvs.model.avro.{FlightRaw, FlightRawWrapper}
import it.bitrock.dvs.streams.KafkaStreamsOptions
import org.apache.kafka.streams.scala.kstream.{Grouped, KStream, Materialized}

object DeduplicateFlightRaw {
  def apply(stream: KStream[String, FlightRaw], kafkaStreamsOptions: KafkaStreamsOptions): KStream[String, FlightRaw] = {
    implicit val groupSerde: Grouped[String, FlightRawWrapper] =
      Grouped.`with`(
        kafkaStreamsOptions.stringKeySerde,
        kafkaStreamsOptions.flightRawWrapperSerde
      )

    stream
      .mapValues(FlightRawWrapper(_, forward = true))
      .groupByKey
      .reduce { (previous, last) =>
        if (last.compareTo(previous) > 0) {
          last
        } else {
          FlightRawWrapper(last.flightRaw, forward = false)
        }
      }(Materialized.`with`(kafkaStreamsOptions.stringKeySerde, kafkaStreamsOptions.flightRawWrapperSerde))
      .filter((_, value) => value.forward)
      .mapValues(_.flightRaw)
      .toStream
  }
}
