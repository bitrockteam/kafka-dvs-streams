package it.bitrock.dvs.streams.topologies

import java.util.Properties

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.config.AppConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder

object FlightEnhancementStream {
  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val keySerde: Serde[String]                   = kafkaStreamsOptions.stringKeySerde
    implicit val flightRawSerde: Serde[FlightRaw]          = kafkaStreamsOptions.flightRawSerde
    implicit val flightEnhanceSerde: Serde[FlightStateRaw] = kafkaStreamsOptions.enhancedFlightSerde

    val streamsBuilder = new StreamsBuilder

    val enhancedFlightTable = streamsBuilder.table[String, FlightStateRaw](config.kafka.topology.flightOpenSkyRawTopic.name)

    streamsBuilder
      .stream[String, FlightRaw](config.kafka.topology.flightRawTopic.name)
      .leftJoin(enhancedFlightTable)(enhanceFlight)
      .to(config.kafka.topology.enhancedFlightRawTopic.name)

    val props = streamProperties(config.kafka, config.kafka.topology.enhancedFlightRawTopic.name)

    List((streamsBuilder.build(props), props))
  }

  private def enhanceFlight(flightRaw: FlightRaw, flightStateRaw: FlightStateRaw): FlightRaw =
    Option(flightStateRaw)
      .filter(_.updated.isAfter(flightRaw.system.updated))
      .map(enhancedFlight =>
        flightRaw.copy(
          geography = enhancedFlight.geography,
          speed = flightRaw.speed.copy(horizontal = enhancedFlight.horizontalSpeed),
          system = System(enhancedFlight.updated)
        )
      )
      .getOrElse(flightRaw)

}
