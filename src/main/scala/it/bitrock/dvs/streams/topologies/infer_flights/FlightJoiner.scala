package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Duration

import it.bitrock.dvs.model.avro.FlightRaw
import it.bitrock.dvs.streams.KafkaStreamsOptions
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.kstream.{KStream, StreamJoined}

object FlightJoiner {
  def apply(
      flightRaw: KStream[String, FlightRaw],
      enriched: KStream[String, FlightRaw],
      opts: KafkaStreamsOptions
  ): KStream[String, FlightRaw] = {
    implicit val joinSerde: StreamJoined[String, FlightRaw, FlightRaw] =
      StreamJoined.`with`(opts.stringKeySerde, opts.flightRawSerde, opts.flightRawSerde)

    flightRaw
      .outerJoin(enriched)(
        (flight, enriched) => if (null != flight) flight else enriched,
        JoinWindows.of(Duration.ofSeconds(5))
      )
  }
}
