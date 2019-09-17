package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model.{CountFlightStatus, FlightEnrichedEvent}

trait SimpleAggregator[K, V, A] {

  val initializer: A
  val adder: (K, V, A) => A
  val subtractor: (K, V, A) => A

}

class CountFlightAggregator extends SimpleAggregator[String, FlightEnrichedEvent, CountFlightStatus] {

  val initializer: CountFlightStatus = CountFlightStatus("", 0)

  val adder: (String, FlightEnrichedEvent, CountFlightStatus) => CountFlightStatus = (k, _, agg) => CountFlightStatus(k, agg.eventCount + 1)

  val subtractor: (String, FlightEnrichedEvent, CountFlightStatus) => CountFlightStatus = (k, _, agg) =>
    CountFlightStatus(k, agg.eventCount - 1)

}
