package it.bitrock.dvs.streams.topologies

import java.util.Properties

import it.bitrock.dvs.model.avro.{FlightReceived, FlightReceivedList}
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.config.AppConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Suppressed.BufferConfig
import org.apache.kafka.streams.scala.kstream.{Produced, Suppressed}
import it.bitrock.dvs.streams.topologies.StreamOps._

object FlightListStream {
  final val AllRecordsKey: String            = "all"
  final val EarthRadius: Double              = 6371.01
  final val MaxDepartureAirportDistance: Int = 10

  sealed trait FlightStatus extends Product with Serializable
  case object Landed        extends FlightStatus
  case object EnRoute       extends FlightStatus

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val StringKeySerde: Serde[String]                           = kafkaStreamsOptions.stringKeySerde
    implicit val IntKeySerde: Serde[Int]                                 = kafkaStreamsOptions.intKeySerde
    implicit val flightReceivedEventSerde: Serde[FlightReceived]         = kafkaStreamsOptions.flightReceivedEventSerde
    implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde

    def partitioner(key: String): Int =
      Math.abs(key.hashCode % config.kafka.topology.flightReceivedPartitionerTopic.partitions)

    val fixedPartitioner: Produced[Int, FlightReceived] =
      Produced.`with`[Int, FlightReceived]((_: String, key: Int, _: FlightReceived, numPartitions: Int) =>
        Some(Integer.valueOf(key)).filter(_ < numPartitions).orNull
      )

    val streamsBuilder = new StreamsBuilder
    val (landedFlights, enRouteFlights) = streamsBuilder
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
      .through(config.kafka.topology.flightReceivedListTopic.name)
      .flatMap { (k, v) =>
        val (g, b) = v.elements.partition(distanceToDestination(_) < MaxDepartureAirportDistance)
        List((k, (Landed, FlightReceivedList(g))), (k, (EnRoute, FlightReceivedList(b))))
      }
      .split(
        (_, v) => v._1 == Landed,
        (_, v) => v._1 == EnRoute
      )

    landedFlights.mapValues(_._2).to(config.kafka.topology.flightLandedListTopic.name)
    enRouteFlights.mapValues(_._2).to(config.kafka.topology.flightEnRouteListTopic.name)

    val props = streamProperties(config.kafka, config.kafka.topology.flightReceivedListTopic.name)
    List((streamsBuilder.build(props), props))
  }

  private def distanceToDestination(flight: FlightReceived): Double = {
    val latRad1 = Math.toRadians(flight.geography.latitude)
    val lonRad1 = Math.toRadians(flight.geography.longitude)
    val latRad2 = Math.toRadians(flight.arrivalAirport.latitude)
    val lonRad2 = Math.toRadians(flight.arrivalAirport.longitude)

    EarthRadius * Math.acos(
      Math.sin(latRad1) * Math.sin(latRad2) + Math.cos(latRad1) * Math.cos(latRad2) * Math.cos(lonRad1 - lonRad2)
    )
  }
}
