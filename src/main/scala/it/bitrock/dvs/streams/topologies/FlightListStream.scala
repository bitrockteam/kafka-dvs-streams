package it.bitrock.dvs.streams.topologies

import java.time.Instant
import java.util.{Properties, UUID}

import it.bitrock.dvs.model.avro.monitoring.FlightReceivedListComputationStatus
import it.bitrock.dvs.model.avro.{FlightReceived, FlightReceivedList}
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.config.AppConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Suppressed
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig

object FlightListStream {

  final val AllRecordsKey: String = "all"

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val KeySerde: Serde[String]                                 = kafkaStreamsOptions.stringKeySerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived]         = kafkaStreamsOptions.flightReceivedEventSerde
    implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde
    implicit val computationStatusSerde: Serde[FlightReceivedListComputationStatus] =
      kafkaStreamsOptions.flightReceivedListComputationStatusSerde

    val streamsBuilder = new StreamsBuilder
    streamsBuilder
      .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic.name)
      .groupBy((_, _) => AllRecordsKey)
      .windowedBy(
        TimeWindows
          .of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize))
          .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
      )
      .aggregate(FlightReceivedList())((_, v, agg) => FlightReceivedList(v +: agg.elements))
      .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
      .toStream
      .map((k, v) => (k.window.start.toString, v))
      .through(config.kafka.topology.flightReceivedListTopic.name)
      .map((k, v) => (UUID.randomUUID().toString, computationStatus(k, v)))
      .to(config.kafka.monitoring.flightReceivedList.topic)

    val props = streamProperties(config.kafka, config.kafka.topology.flightReceivedListTopic.name)
    List((streamsBuilder.build(props), props))

  }

  private def computationStatus(windowStart: String, v: FlightReceivedList): FlightReceivedListComputationStatus =
    FlightReceivedListComputationStatus(
      windowTime = Instant.ofEpochMilli(windowStart.toLong),
      emissionTime = Instant.now(),
      minUpdated = v.elements.minBy(_.updated).updated,
      maxUpdated = v.elements.maxBy(_.updated).updated,
      windowElements = v.elements.size
    )

}
