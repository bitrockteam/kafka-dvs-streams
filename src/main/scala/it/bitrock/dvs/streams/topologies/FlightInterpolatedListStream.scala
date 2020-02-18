package it.bitrock.dvs.streams.topologies

import java.util.Properties

import it.bitrock.dvs.model.avro.{FlightReceived, FlightReceivedList}
import it.bitrock.dvs.streams.StreamProps.streamProperties
import it.bitrock.dvs.streams._
import it.bitrock.dvs.streams.config.AppConfig
import it.bitrock.dvs.streams.geo.utils.EarthPositionCalculator
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

  def buildTopology(config: AppConfig, kafkaStreamsOptions: KafkaStreamsOptions): List[(Topology, Properties)] = {
    implicit val stringKeySerde: Serde[String]                           = kafkaStreamsOptions.stringKeySerde
    implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde

    val streamsBuilder = new StreamsBuilder

    val stateStore = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("FlightListInterpolationStateStore"),
      stringKeySerde,
      flightReceivedListEventSerde
    )

    streamsBuilder.addStateStore(stateStore)

    streamsBuilder
      .stream[String, FlightReceivedList](config.kafka.topology.flightEnRouteListTopic.name)
      .transform[String, FlightReceivedList](
        () => interpolationTransformer(stateStore.name, config.kafka.topology.interpolationInterval),
        stateStore.name
      )
      .to(config.kafka.topology.flightInterpolatedListTopic.name)

    val props = streamProperties(config.kafka, config.kafka.topology.flightReceivedListTopic.name)
    List((streamsBuilder.build(props), props))
  }

  @SuppressWarnings(Array("DisableSyntax.var"))
  private def interpolationTransformer(
      stateStoreName: String,
      interpolationInterval: FiniteDuration
  ): Transformer[String, FlightReceivedList, KeyValue[String, FlightReceivedList]] =
    new Transformer[String, FlightReceivedList, KeyValue[String, FlightReceivedList]] {
      private var processorContext: ProcessorContext                       = _
      private var keyValueStore: KeyValueStore[String, FlightReceivedList] = _
      private var scheduledTask: Cancellable                               = _

      override def init(context: ProcessorContext): Unit = {
        processorContext = context
        keyValueStore = context.getStateStore(stateStoreName).asInstanceOf[KeyValueStore[String, FlightReceivedList]]
        scheduledTask = context.schedule(
          duration2JavaDuration(interpolationInterval),
          PunctuationType.WALL_CLOCK_TIME,
          interpolationPunctuator(context, keyValueStore)
        )
      }

      override def transform(key: String, value: FlightReceivedList): KeyValue[String, FlightReceivedList] = {
        keyValueStore.put(currentSnapshot, value)
        KeyValue.pair(key, value)
      }

      override def close(): Unit = scheduledTask.cancel()

    }

  private def interpolationPunctuator(
      processorContext: ProcessorContext,
      keyValueStore: KeyValueStore[String, FlightReceivedList]
  ): Punctuator =
    (timestamp: Long) =>
      processorContext.forward(
        timestamp.toString,
        FlightReceivedList(keyValueStore.get(currentSnapshot).elements.map(f => interpolateFlight(f, timestamp)))
      )

  private def interpolateFlight(flight: FlightReceived, currentTime: Long): FlightReceived = {
    val distance = flight.speed * (currentTime - flight.updated.toEpochMilli).millis.toHours / 1000
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
