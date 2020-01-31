package it.bitrock.dvs.streams.topologies

import java.time.Instant
import java.time.temporal.ChronoUnit

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams.CommonSpecUtils._
import it.bitrock.dvs.streams.TestValues
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.testcommons.Suite
import net.manub.embeddedkafka.schemaregistry._
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import org.apache.kafka.common.serialization.Serde
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike

class FlightListStreamSpec extends Suite with AnyWordSpecLike with EmbeddedKafkaStreams with OptionValues with TestValues {

  "FlightListStream" should {

    "produce FlightReceivedList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.keySerde

        val key1 = "a"
        val firstMessage = key1 -> FlightReceivedEvent.copy(
          iataNumber = key1,
          icaoNumber = key1,
          updated = Instant.now().minus(1, ChronoUnit.SECONDS)
        )

        val key2 = "b"
        val secondMessage = key2 -> FlightReceivedEvent.copy(
          iataNumber = key2,
          icaoNumber = key2
        )

        val key3 = "c"
        val thirdMessage = key3 -> FlightReceivedEvent.copy(
          iataNumber = key3,
          icaoNumber = key3,
          updated = Instant.now().plus(1, ChronoUnit.SECONDS)
        )

        val flightMessages = List(firstMessage, secondMessage, thirdMessage)

        val receivedRecords = ResourceLoaner.runAll(topologies(FlightListTopology)) { _ =>
          publishToKafka(appConfig.kafka.topology.flightReceivedTopic, flightMessages)
          publishToKafka(dummyFlightReceivedForcingSuppression(appConfig.kafka.topology.flightReceivedTopic))

          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedList](
            topics = Set(appConfig.kafka.topology.flightReceivedListTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )

          val computationStatusMessagesMap = consumeNumberKeyedMessagesFromTopics[String, ComputationStatus](
            topics = Set(appConfig.kafka.topology.computationStatusTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )

          (
            messagesMap(appConfig.kafka.topology.flightReceivedListTopic).map(_._2),
            computationStatusMessagesMap(appConfig.kafka.topology.computationStatusTopic).map(_._2)
          )
        }

        val (flightsReceived, computationStatus) = receivedRecords

        flightsReceived should have size 1
        flightsReceived.head.elements should contain theSameElementsAs flightMessages.map(_._2)

        computationStatus should have size 1
        val cs = computationStatus.head
        cs.minUpdated shouldBe firstMessage._2.updated
        cs.maxUpdated shouldBe thirdMessage._2.updated
    }

  }

}
