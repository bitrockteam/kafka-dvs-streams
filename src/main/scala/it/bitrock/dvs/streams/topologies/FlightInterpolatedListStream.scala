package it.bitrock.dvs.streams.topologies

import java.time.{Clock, Instant}
import java.util.Properties

import it.bitrock.dvs.model.avro.{FlightInterpolated, FlightInterpolatedList, FlightReceived, FlightReceivedList}
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.config.AppConfig
import it.bitrock.dvs.streams.geo.utils.EarthPositionCalculator
import it.bitrock.dvs.streams.mapper.FlightInterpolatedMapper
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KeyValue, Topology}

import scala.concurrent.duration._

object FlightInterpolatedListStream {
  private val currentSnapshot = "currentSnapshot"

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions)(
      implicit clock: Clock
  ): List[(Topology, Properties)] = {
    implicit val stringKeySerde: Serde[String]                           = kafkaStreamsOptions.stringKeySerde
    implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde
    implicit val flightInterpolatedListEventSerde: Serde[FlightInterpolatedList] =
      kafkaStreamsOptions.flightInterpolatedListEventSerde

    val streamsBuilder = new StreamsBuilder

    val flightListInterpolationStateStoreName = "FlightListInterpolationStateStore"
    val stateStore = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore(flightListInterpolationStateStoreName),
      stringKeySerde,
      flightReceivedListEventSerde
    )

    streamsBuilder.addStateStore(stateStore)

    streamsBuilder
      .stream[String, FlightReceivedList](config.kafka.topology.flightEnRouteListTopic.name)
      .transform[String, FlightInterpolatedList](
        () => interpolationTransformer(stateStore.name, config.kafka.topology.interpolationInterval),
        stateStore.name
      )
      .to(config.kafka.topology.flightInterpolatedListTopic.name)

    val props = streamProperties(config.kafka, config.kafka.topology.flightInterpolatedListTopic.name)
    List((streamsBuilder.build(props), props))
  }

  @SuppressWarnings(Array("DisableSyntax.var"))
  private def interpolationTransformer(
      stateStoreName: String,
      interpolationInterval: FiniteDuration
  )(implicit clock: Clock): Transformer[String, FlightReceivedList, KeyValue[String, FlightInterpolatedList]] =
    new Transformer[String, FlightReceivedList, KeyValue[String, FlightInterpolatedList]] {
      private var keyValueStore: KeyValueStore[String, FlightReceivedList] = _
      private var scheduledTask: Cancellable                               = _

      override def init(context: ProcessorContext): Unit = {
        keyValueStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, FlightReceivedList]]
        scheduledTask = context.schedule(
          duration2JavaDuration(interpolationInterval),
          PunctuationType.WALL_CLOCK_TIME,
          interpolationPunctuator(context, keyValueStore)
        )
      }

      override def transform(key: String, value: FlightReceivedList): KeyValue[String, FlightInterpolatedList] = {
        keyValueStore.put(currentSnapshot, value)
        val now = Instant.now(clock).toEpochMilli
        KeyValue.pair(now.toString, FlightInterpolatedList(value.elements.map((f: FlightReceived) => interpolateFlight(f, now))))
      }

      override def close(): Unit = scheduledTask.cancel()

      private def interpolationPunctuator(
          processorContext: ProcessorContext,
          keyValueStore: KeyValueStore[String, FlightReceivedList]
      ): Punctuator =
        (_: Long) =>
          Option(keyValueStore.get(currentSnapshot)).foreach { data =>
            val now = Instant.now(clock).toEpochMilli
            processorContext.forward(
              now.toString,
              FlightInterpolatedList(data.elements.map((f: FlightReceived) => interpolateFlight(f, now)))
            )
          }
    }

  private def interpolateFlight(flight: FlightReceived, currentTime: Long): FlightInterpolated = {
    val distance = kmPerHoursToMetersPerMillis(flight.speed) * (currentTime - flight.updated.toEpochMilli)
    val newPosition = EarthPositionCalculator.position(
      latitude = flight.geography.latitude,
      longitude = flight.geography.longitude,
      altitude = flight.geography.altitude,
      distance = distance,
      direction = flight.geography.direction
    )
    val newGeography = flight.geography.copy(latitude = newPosition.latitude, longitude = newPosition.longitude)
    FlightInterpolatedMapper.interpolatedWith(newGeography, Instant.ofEpochMilli(currentTime))(flight)
  }

  private def kmPerHoursToMetersPerMillis(speed: Double): Double = speed / 60 / 60

}
