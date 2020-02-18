package it.bitrock.dvs.streams.topologies

import java.util.Properties

import it.bitrock.dvs.model.avro.{FlightReceived, FlightReceivedList}
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.config.AppConfig
import it.bitrock.dvs.streams.geo.utils.EarthPositionCalculator
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.concurrent.duration._

object FlightListInterpolationStream {

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val StringKeySerde: Serde[String]                           = kafkaStreamsOptions.stringKeySerde
    implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde

    val streamsBuilder = new StreamsBuilder
    streamsBuilder
      .stream[String, FlightReceivedList](config.kafka.topology.flightEnRouteListTopic.name)
      .flatMapValues(v =>
        (0 until 30 by 5).map(interval => v.copy(elements = v.elements.map(f => projectedFlight(f, interval.seconds))))
      )
      .to(config.kafka.topology.flightProjectedListTopic.name)

    val props = streamProperties(config.kafka, config.kafka.topology.flightProjectedListTopic.name)
    List((streamsBuilder.build(props), props))
  }

  private def projectedFlight(flight: FlightReceived, time: FiniteDuration): FlightReceived = {
    val distance = flight.speed * time.toHours / 1000
    val newPosition = EarthPositionCalculator.position(
      latitude = flight.geography.latitude,
      longitude = flight.geography.longitude,
      altitude = flight.geography.altitude,
      distance = distance,
      direction = flight.geography.direction
    )
    val newGeography = flight.geography.copy(latitude = newPosition.latitude, longitude = newPosition.longitude)
    flight.copy(geography = newGeography)
  }
}
