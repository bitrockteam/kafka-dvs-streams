package it.bitrock.dvs.streams.topologies.infer_flights

import java.time.Duration
import java.util.Properties

import it.bitrock.dvs.model.avro.FlightRaw
import it.bitrock.dvs.streams.KafkaStreamsOptions
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams.config.AppConfig
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

    val streamsBuilder      = new StreamsBuilder
    val flightRawTopic      = config.kafka.topology.flightRawTopic.name
    val inferredFlightTopic = config.kafka.topology.inferredFlightRawTopic.name

    val inferFlightsStore = Stores.timestampedKeyValueStoreBuilder(
      Stores.inMemoryKeyValueStore("infer-flights"),
      Serdes.String,
      flightRawEventSerde
    )
    val emitterStore = Stores.timestampedKeyValueStoreBuilder(
      Stores.inMemoryKeyValueStore("emitter"), // TODO: use a persistent one here
      Serdes.String,
      flightRawEventSerde
    )

    streamsBuilder.addStateStore(inferFlightsStore)
    streamsBuilder.addStateStore(emitterStore)

    val flightRawStream      = streamsBuilder.stream[String, FlightRaw](flightRawTopic)
    val inferredFlightStream = streamsBuilder.stream[String, FlightRaw](inferredFlightTopic)

    applyTopology(
      flightRawStream,
      inferredFlightStream,
      kafkaStreamsOptions,
      inferFlightsStore.name(), emitterStore.name()
    ).to(inferredFlightTopic)

    val props = streamProperties(config.kafka, inferredFlightTopic)
    List((streamsBuilder.build(props), props))
  }

  def applyTopology(
      flightRaw: KStream[String, FlightRaw],
      flightEnriched: KStream[String, FlightRaw],
      kafkaStreamsOptions: KafkaStreamsOptions,
      inferFlightsStoreName: String, emitterStoreName: String
  ): KStream[String, FlightRaw] = {
    val uniqueFlightRaw = DeduplicateFlightRaw(flightRaw, kafkaStreamsOptions)
    val joined          = FlightJoiner(uniqueFlightRaw, flightEnriched, kafkaStreamsOptions)

    val inferredFlights = InferFlight(joined, timeInterval, inferFlightsStoreName)

    val controlledRateStream = ControlledEmitter.transformStream(
      emitterStoreName,
      timeInterval,
      inferredFlights
    )

    controlledRateStream
    // .filter((_, flight) => flight.accuracy <= iterationLimit)
  }
}
