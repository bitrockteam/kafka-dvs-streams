package it.bitrock.dvs.streams.topologies.infer_flights

import it.bitrock.dvs.model.avro.FlightReceived
import it.bitrock.dvs.streams.CommonSpecUtils.{ConsumerPollTimeout, InferFlightsTopologyId, Resource, ResourceLoaner}
import it.bitrock.dvs.streams.{CommonSpecUtils, TestValues}
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.testcommons.Suite
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafkaConfig, _}
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import org.apache.kafka.common.serialization.Serde
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike

class InferFlightsTopologySpec extends Suite with AnyWordSpecLike with EmbeddedKafkaStreams with OptionValues with TestValues {
  "InferFlightsTopology" should {
    "generate the expected events" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val receivedRecords = ResourceLoaner.runAll(topologies(InferFlightsTopologyId)) { _ =>
          publishToKafka(appConfig.kafka.topology.flightRawTopic.name, FlightRawEvent.flight.icaoNumber, FlightRawEvent)

          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceived](
            topics = Set(appConfig.kafka.topology.inferredFlightRawTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          val msg = messagesMap(appConfig.kafka.topology.flightReceivedTopic.name).head
          println(msg._2)
        }
    }
  }
}
