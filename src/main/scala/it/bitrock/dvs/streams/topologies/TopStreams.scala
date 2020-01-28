package it.bitrock.dvs.streams.topologies

import java.util.Properties

import it.bitrock.dvs.model.avro._
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

object TopStreams {

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val KeySerde: Serde[String]                                  = kafkaStreamsOptions.keySerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived]          = kafkaStreamsOptions.flightReceivedEventSerde
    implicit val topAggregationKeySerde: Serde[Long]                      = kafkaStreamsOptions.topAggregationKeySerde
    implicit val topArrivalAirportListSerde: Serde[TopArrivalAirportList] = kafkaStreamsOptions.topArrivalAirportListEventSerde
    implicit val topDepartureAirportListSerde: Serde[TopDepartureAirportList] =
      kafkaStreamsOptions.topDepartureAirportListEventSerde
    implicit val topSpeedListSerde: Serde[TopSpeedList]     = kafkaStreamsOptions.topSpeedListEventSerde
    implicit val topAirlineListSerde: Serde[TopAirlineList] = kafkaStreamsOptions.topAirlineListEventSerde
    implicit val topAirportSerde: Serde[Airport]            = kafkaStreamsOptions.topAirportEventSerde
    implicit val topSpeedSerde: Serde[SpeedFlight]          = kafkaStreamsOptions.topSpeedFlightEventSerde
    implicit val topAirlineSerde: Serde[Airline]            = kafkaStreamsOptions.topAirlineEventSerde

    def buildTopArrivalStreamsBuilder: (StreamsBuilder, String) = {
      val streamsBuilder              = new StreamsBuilder
      val topArrivalAirportAggregator = new TopArrivalAirportAggregator(config.topElementsAmount)

      streamsBuilder
        .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic)
        .groupBy((_, v) => v.airportArrival.codeAirport)
        .windowedBy(
          TimeWindows
            .of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize))
            .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
        )
        .count
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .groupBy((k, v) => (k.window.start.toString, Airport(k.key, v)))
        .aggregate(topArrivalAirportAggregator.initializer)(
          topArrivalAirportAggregator.adder,
          topArrivalAirportAggregator.subtractor
        )
        .toStream
        .to(config.kafka.topology.topArrivalAirportTopic)
      (streamsBuilder, config.kafka.topology.topArrivalAirportTopic)
    }

    def buildTopDepartureStreamsBuilder: (StreamsBuilder, String) = {
      val streamsBuilder                = new StreamsBuilder
      val topDepartureAirportAggregator = new TopDepartureAirportAggregator(config.topElementsAmount)

      streamsBuilder
        .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic)
        .groupBy((_, v) => v.airportDeparture.codeAirport)
        .windowedBy(
          TimeWindows
            .of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize))
            .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
        )
        .count
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .groupBy((k, v) => (k.window.start.toString, Airport(k.key, v)))
        .aggregate(topDepartureAirportAggregator.initializer)(
          topDepartureAirportAggregator.adder,
          topDepartureAirportAggregator.subtractor
        )
        .toStream
        .to(config.kafka.topology.topDepartureAirportTopic)
      (streamsBuilder, config.kafka.topology.topDepartureAirportTopic)
    }

    def buildTopFlightSpeedStreamsBuilder: (StreamsBuilder, String) = {
      val streamsBuilder           = new StreamsBuilder
      val topSpeedFlightAggregator = new TopSpeedFlightAggregator(config.topElementsAmount)

      streamsBuilder
        .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic)
        .groupByKey
        .windowedBy(
          TimeWindows
            .of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize))
            .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
        )
        .reduce((_, v2) => v2)
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .groupBy((k, v) => (k.window.start.toString, SpeedFlight(k.key, v.speed)))
        .aggregate(topSpeedFlightAggregator.initializer)(topSpeedFlightAggregator.adder, topSpeedFlightAggregator.subtractor)
        .toStream
        .to(config.kafka.topology.topSpeedTopic)
      (streamsBuilder, config.kafka.topology.topSpeedTopic)
    }

    def buildTopAirlineStreamsBuilder: (StreamsBuilder, String) = {
      val streamsBuilder       = new StreamsBuilder
      val topAirlineAggregator = new TopAirlineAggregator(config.topElementsAmount)

      streamsBuilder
        .stream[String, FlightReceived](config.kafka.topology.flightReceivedTopic)
        .groupBy((_, v) => v.airline.nameAirline)
        .windowedBy(
          TimeWindows
            .of(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowSize))
            .grace(duration2JavaDuration(config.kafka.topology.aggregationTimeWindowGrace))
        )
        .count
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .groupBy((k, v) => (k.window.start.toString, Airline(k.key, v)))
        .aggregate(topAirlineAggregator.initializer)(topAirlineAggregator.adder, topAirlineAggregator.subtractor)
        .toStream
        .to(config.kafka.topology.topAirlineTopic)
      (streamsBuilder, config.kafka.topology.topAirlineTopic)
    }

    val builders = List(
      buildTopArrivalStreamsBuilder,
      buildTopDepartureStreamsBuilder,
      buildTopFlightSpeedStreamsBuilder,
      buildTopAirlineStreamsBuilder
    )

    builders.map {
      case (builder, topic) =>
        val props = streamProperties(config.kafka, topic)
        (builder.build(props), props)
    }

  }
}
