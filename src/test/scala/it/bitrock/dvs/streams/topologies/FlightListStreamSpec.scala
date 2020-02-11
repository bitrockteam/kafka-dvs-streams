package it.bitrock.dvs.streams.topologies

import java.time.Instant
import java.time.temporal.ChronoUnit

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.model.avro.monitoring.FlightReceivedListComputationStatus
import it.bitrock.dvs.streams.CommonSpecUtils._
import it.bitrock.dvs.streams.TestValues
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.testcommons.Suite
import net.manub.embeddedkafka.schemaregistry._
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import org.apache.kafka.common.serialization.Serde
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class FlightListStreamSpec extends Suite with AnyWordSpecLike with EmbeddedKafkaStreams with OptionValues with TestValues {

  "FlightListStream" should {

    "produce FlightReceivedList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val now = Instant.now()

        val key1 = "a"
        val firstMessage = key1 -> FlightReceivedEvent.copy(
          iataNumber = key1,
          icaoNumber = key1,
          updated = now.minus(1, ChronoUnit.SECONDS)
        )

        val key2 = "b"
        val secondMessage = key2 -> FlightReceivedEvent.copy(
          iataNumber = key2,
          icaoNumber = key2,
          updated = now
        )

        val key3 = "c"
        val thirdMessage = key3 -> FlightReceivedEvent.copy(
          iataNumber = key3,
          icaoNumber = key3,
          updated = now.plus(1, ChronoUnit.SECONDS)
        )

        val flightMessages = List(firstMessage, secondMessage, thirdMessage)

        val topicsToCreate =
          List(
            appConfig.kafka.topology.flightReceivedTopic.name,
            appConfig.kafka.topology.flightReceivedPartitionerTopic.name,
            appConfig.kafka.topology.flightReceivedListTopic.name,
            appConfig.kafka.monitoring.flightReceivedList.topic
          )

        val receivedRecords = ResourceLoaner.runAll(topologies(FlightListTopology), topicsToCreate) { _ =>
          publishToKafka(appConfig.kafka.topology.flightReceivedTopic.name, flightMessages)
          publishToKafka(dummyFlightReceivedForcingSuppression(appConfig.kafka.topology.flightReceivedTopic.name))
          publishToKafka(
            dummyFlightReceivedForcingSuppression(appConfig.kafka.topology.flightReceivedTopic.name, 2.minutes)
          )

          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedList](
            topics = Set(appConfig.kafka.topology.flightReceivedListTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )

          val computationStatusMessagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedListComputationStatus](
            topics = Set(appConfig.kafka.monitoring.flightReceivedList.topic),
            number = 1,
            timeout = ConsumerPollTimeout
          )

          (
            messagesMap(appConfig.kafka.topology.flightReceivedListTopic.name).map(_._2),
            computationStatusMessagesMap(appConfig.kafka.monitoring.flightReceivedList.topic).map(_._2)
          )
        }

        val (flightsReceived, computationStatus) = receivedRecords

        flightsReceived should have size 1
        flightsReceived.head.elements should contain theSameElementsAs flightMessages.map(_._2)

        computationStatus should have size 1
        val cs = computationStatus.head
        cs.minUpdated shouldBe firstMessage._2.updated
        cs.maxUpdated shouldBe thirdMessage._2.updated
        cs.averageUpdated shouldBe now
        cs.windowElements shouldBe 3
    }

  }

}
