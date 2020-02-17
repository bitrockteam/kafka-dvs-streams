package it.bitrock.dvs.streams.topologies.monitoring

import java.time.Instant
import java.util.{Properties, UUID}

import it.bitrock.dvs.model.avro.FlightReceivedList
import it.bitrock.dvs.model.avro.monitoring.FlightReceivedListComputationStatus
import it.bitrock.dvs.streams.KafkaStreamsOptions
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams.config.AppConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.concurrent.duration._

object FlightReceivedListComputationStatusStreams {
  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val keySerde: Serde[String]                                 = kafkaStreamsOptions.stringKeySerde
    implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde
    implicit val computationStatusSerde: Serde[FlightReceivedListComputationStatus] =
      kafkaStreamsOptions.flightReceivedListComputationStatusSerde

    val streamsBuilder = new StreamsBuilder

    streamsBuilder
      .stream[String, FlightReceivedList](config.kafka.topology.flightReceivedListTopic.name)
      .map((k, v) => (UUID.randomUUID().toString, computationStatus(k, v)))
      .through(config.kafka.monitoring.flightReceivedList.topic)
      .filterNot((_, v) => delayInRange(v, config.kafka.monitoring.flightReceivedList.allowedDelay))
      .to(config.kafka.monitoring.flightReceivedList.delayTopic)

    val props = streamProperties(config.kafka, config.kafka.monitoring.flightReceivedList.topic)
    List((streamsBuilder.build(props), props))
  }

  private def computationStatus(windowStart: String, v: FlightReceivedList): FlightReceivedListComputationStatus = {
    val elements = v.elements.size
    val average  = v.elements.map(_.updated.toEpochMilli).sum / elements
    FlightReceivedListComputationStatus(
      windowTime = Instant.ofEpochMilli(windowStart.toLong),
      emissionTime = Instant.now(),
      minUpdated = v.elements.minBy(_.updated).updated,
      maxUpdated = v.elements.maxBy(_.updated).updated,
      averageUpdated = Instant.ofEpochMilli(average),
      windowElements = elements
    )
  }

  private def delayInRange(computationStatus: FlightReceivedListComputationStatus, allowedDelay: FiniteDuration): Boolean =
    (computationStatus.emissionTime.toEpochMilli - computationStatus.windowTime.toEpochMilli).millis < allowedDelay
}
