package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Duration

import it.bitrock.dvs.model.avro.FlightRaw
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.kstream.{KStream, StreamJoined}

object FlightJoiner {
  implicit val joinSerde: StreamJoined[String, FlightRaw, FlightRaw] = StreamJoined.`with`(keySerde, flightRawSerde, flightRawSerde)

  def apply(flightRaw: KStream[String, FlightRaw], enriched: KStream[String, FlightRaw]): KStream[String, FlightRaw] = {
    flightRaw
      .outerJoin(enriched)(
        (flight, enriched) => if (null != flight) flight else enriched,
        JoinWindows.of(Duration.ofSeconds(5))
      )
  }
}
