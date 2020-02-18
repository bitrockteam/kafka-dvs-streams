package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Duration
import java.util.Properties

import it.bitrock.dvs.model.avro.FlightRaw
import it.bitrock.dvs.streams.KafkaStreamsOptions
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams.config.AppConfig
import it.bitrock.dvs.streams.topologies.infer_flights.model.FlightRawTs
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.Stores

object InferFlightsTopology {
  private val timeInterval: Duration = Duration.ofSeconds(5)

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val keySerde: Serde[String]               = kafkaStreamsOptions.stringKeySerde
    implicit val flightRawEventSerde: Serde[FlightRaw] = kafkaStreamsOptions.flightRawSerde
    implicit val flightRawConsumed: Consumed[String, FlightRaw] =
      Consumed.`with`[String, FlightRaw](keySerde, flightRawEventSerde)
    implicit val flightRawProduced: Produced[String, FlightRaw] =
      Produced.`with`[String, FlightRaw](keySerde, flightRawEventSerde)
    val flightRawTsSerde: Serde[FlightRawTs] = Serdes.serdeFrom(classOf[FlightRawTs])

    val streamsBuilder      = new StreamsBuilder
    val flightRawTopic      = config.kafka.topology.flightRawTopic.name
    val inferredFlightTopic = config.kafka.topology.inferredFlightRawTopic.name

    val flightRawStream      = streamsBuilder.stream[String, FlightRaw](flightRawTopic)
    val inferredFlightStream = streamsBuilder.stream[String, FlightRaw](inferredFlightTopic)
    applyTopology(flightRawStream, inferredFlightStream, kafkaStreamsOptions).to(inferredFlightTopic)

    streamsBuilder
      .addStateStore(
        Stores.keyValueStoreBuilder(
          Stores.inMemoryKeyValueStore("infer-flights"),
          Serdes.String,
          flightRawTsSerde
        )
      )
    streamsBuilder
      .addStateStore(
        Stores.keyValueStoreBuilder(
          Stores.inMemoryKeyValueStore("emitter"), // TODO: use a persistent one here
          Serdes.String,
          flightRawTsSerde
        )
      )

    val props = streamProperties(config.kafka, inferredFlightTopic)
    List((streamsBuilder.build(props), props))
  }

  def applyTopology(
      flightRaw: KStream[String, FlightRaw],
      flightEnriched: KStream[String, FlightRaw],
      kafkaStreamsOptions: KafkaStreamsOptions
  ): KStream[String, FlightRaw] = {
    val uniqueFlightRaw = DeduplicateFlightRaw(flightRaw, kafkaStreamsOptions)
    val joined          = FlightJoiner(uniqueFlightRaw, flightEnriched, kafkaStreamsOptions)

    val inferredFlightsWithTs = InferFlight(joined, timeInterval, "infer-flights")

    val controlledRateStream = ControlledEmitter.transformStream(
      "emitter",
      timeInterval,
      inferredFlightsWithTs
    )

    controlledRateStream
      .mapValues(_.flightRaw)
    // .filter((_, flight) => flight.accuracy <= iterationLimit)
  }
}
