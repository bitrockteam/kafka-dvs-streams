package it.bitrock.dvs.streams.topologies

import java.util.Properties

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams.config.AppConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Suppressed
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig

object TotalStreams {

  final val AllRecordsKey: String = "all"

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val KeySerde: Serde[String]                         = kafkaStreamsOptions.keySerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived] = kafkaStreamsOptions.flightReceivedEventSerde
    implicit val countFlightSerde: Serde[CountFlight]            = kafkaStreamsOptions.countFlightEventSerde
    implicit val countAirlineSerde: Serde[CountAirline]          = kafkaStreamsOptions.countAirlineEventSerde
    implicit val codeAirlineListSerde: Serde[CodeAirlineList]    = kafkaStreamsOptions.codeAirlineListEventSerde
    implicit val flightNumberListSerde: Serde[FlightNumberList]  = kafkaStreamsOptions.flightNumberListEventSerde

    def buildTotalFlightsStreamsBuilder: (StreamsBuilder, String) = {
      val streamsBuilder = new StreamsBuilder
      streamsBuilder
        .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic)
        .groupBy((_, _) => AllRecordsKey)
        .windowedBy(
          TimeWindows
            .of(duration2JavaDuration(config.kafka.topology.aggregationTotalTimeWindowSize))
            .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
        )
        .aggregate(FlightNumberList())((_, v, agg) => FlightNumberList(agg.elements :+ v.icaoNumber))
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .toStream
        .map((k, v) => (k.window.start.toString, CountFlight(k.window.start.toString, v.elements.distinct.size)))
        .to(config.kafka.topology.totalFlightTopic)
      (streamsBuilder, config.kafka.topology.totalFlightTopic)
    }

    def buildTotalAirlinesStreamsBuilder: (StreamsBuilder, String) = {
      val streamsBuilder = new StreamsBuilder
      streamsBuilder
        .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic)
        .groupBy((_, _) => AllRecordsKey)
        .windowedBy(
          TimeWindows
            .of(duration2JavaDuration(config.kafka.topology.aggregationTotalTimeWindowSize))
            .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
        )
        .aggregate(CodeAirlineList())((_, v, agg) => CodeAirlineList(agg.elements :+ v.airline.codeAirline))
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .toStream
        .map((k, v) => (k.window.start.toString, CountAirline(k.window.start.toString, v.elements.distinct.size)))
        .to(config.kafka.topology.totalAirlineTopic)
      (streamsBuilder, config.kafka.topology.totalAirlineTopic)
    }

    val builders = List(
      buildTotalFlightsStreamsBuilder,
      buildTotalAirlinesStreamsBuilder
    )

    builders.map {
      case (builder, topic) =>
        val props = streamProperties(config.kafka, topic)
        (builder.build(props), props)
    }

  }
}
