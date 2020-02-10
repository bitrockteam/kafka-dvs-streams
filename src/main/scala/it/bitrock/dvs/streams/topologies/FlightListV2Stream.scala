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
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.scala.kstream.{Produced, Suppressed}
import org.apache.kafka.streams.scala.StreamsBuilder

object FlightListV2Stream {

  final val AllRecordsKey: String = "all"

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val StringKeySerde: Serde[String]                           = kafkaStreamsOptions.stringKeySerde
    implicit val IntKeySerde: Serde[Int]                                 = kafkaStreamsOptions.intKeySerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived]         = kafkaStreamsOptions.flightReceivedEventSerde
    implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde
    implicit val computationStatusSerde: Serde[FlightReceivedListComputationStatus] =
      kafkaStreamsOptions.flightReceivedListComputationStatusSerde

    def partitioner(key: String): Int =
      Math.abs(key.hashCode % config.kafka.topology.flightReceivedPartitionerTopic.partitions)

    val fixedPartitioner: Produced[Int, FlightReceived] = Produced.`with`[Int, FlightReceived](
      (_: String, key: Int, _: FlightReceived, numPartitions: Int) => Some(Integer.valueOf(key)).filter(_ < numPartitions).orNull
    )

    val streamsBuilder = new StreamsBuilder
    streamsBuilder
      .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic.name)
      .selectKey((k, _) => partitioner(k))
      .through(config.kafka.topology.flightReceivedPartitionerTopic.name)(fixedPartitioner)
      .groupByKey
      .windowedBy(
        TimeWindows
          .of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize))
          .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
      )
      .aggregate(FlightReceivedList())((_, v, agg) => FlightReceivedList(v +: agg.elements))
      .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
      .toStream
      .groupBy((_, _) => AllRecordsKey)
      .windowedBy(
        TimeWindows
          .of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize))
          .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
      )
      .aggregate(FlightReceivedList())((_, v, agg) => FlightReceivedList(v.elements ++ agg.elements))
      .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
      .toStream
      .map((k, v) => (k.window.start.toString, v))
      .through(config.kafka.topology.flightReceivedListV2Topic.name)
      .map((k, v) => (UUID.randomUUID().toString, computationStatus(k, v)))
      .to(config.kafka.monitoring.flightReceivedListV2)

    val props = streamProperties(config.kafka, config.kafka.topology.flightReceivedListV2Topic.name)
    List((streamsBuilder.build(props), props))

  }

  private def computationStatus(windowStart: String, v: FlightReceivedList): FlightReceivedListComputationStatus =
    FlightReceivedListComputationStatus(
      Instant.ofEpochMilli(windowStart.toLong),
      Instant.now(),
      v.elements.minBy(_.updated).updated,
      v.elements.maxBy(_.updated).updated,
      v.elements.size
    )

}
