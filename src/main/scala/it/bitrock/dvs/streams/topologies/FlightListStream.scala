package it.bitrock.dvs.streams.topologies

import java.util.Properties

import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams.config.AppConfig
import it.bitrock.dvs.model.avro._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder

object FlightListStream {

  final val AllRecordsKey: String = "all"

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val KeySerde: Serde[String]                                 = kafkaStreamsOptions.keySerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived]         = kafkaStreamsOptions.flightReceivedEventSerde
    implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde

    val streamsBuilder = new StreamsBuilder
    streamsBuilder
      .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic)
      .groupBy((_, _) => AllRecordsKey)
      .windowedBy(TimeWindows.of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize)))
      .aggregate(FlightReceivedList())((_, v, agg) => FlightReceivedList(agg.elements :+ v))
      .toStream
      .map((k, v) => (k.window.start.toString, v))
      .to(config.kafka.topology.flightReceivedListTopic)

    val props = streamProperties(config.kafka, config.kafka.topology.flightReceivedListTopic)
    List((streamsBuilder.build(props), props))

  }

}
