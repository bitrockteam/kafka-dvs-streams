package it.bitrock.dvs.streams.topologies.monitoring

import java.util.Properties

import it.bitrock.dvs.model.avro.monitoring.FlightReceivedListComputationStatus
import it.bitrock.dvs.streams.KafkaStreamsOptions
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams.config.AppConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.concurrent.duration.{FiniteDuration, _}

object FlightReceivedListComputationStatusStreams {

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val keySerde: Serde[String] = kafkaStreamsOptions.keySerde
    implicit val computationStatusSerde: Serde[FlightReceivedListComputationStatus] =
      kafkaStreamsOptions.flightReceivedListComputationStatusSerde

    val streamsBuilder = new StreamsBuilder
    streamsBuilder
      .stream[String, FlightReceivedListComputationStatus](config.kafka.monitoring.flightReceivedList.topic)
      .filterNot((_, v) => delayInRange(v, config.kafka.monitoring.flightReceivedList.allowedDelay))
      .to(config.kafka.monitoring.flightReceivedList.delayTopic)

    val props = streamProperties(config.kafka, config.kafka.monitoring.flightReceivedList.topic)
    List((streamsBuilder.build(props), props))
  }

  private def delayInRange(computationStatus: FlightReceivedListComputationStatus, allowedDelay: FiniteDuration): Boolean =
    (computationStatus.emissionTime.toEpochMilli - computationStatus.windowTime.toEpochMilli).millis < allowedDelay
}
