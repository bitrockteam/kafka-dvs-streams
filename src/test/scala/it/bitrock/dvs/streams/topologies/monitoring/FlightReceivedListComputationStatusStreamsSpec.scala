package it.bitrock.dvs.streams.topologies.monitoring

import java.time.Instant
import java.time.temporal.ChronoUnit

import it.bitrock.dvs.model.avro.FlightReceivedList
import it.bitrock.dvs.model.avro.monitoring.FlightReceivedListComputationStatus
import it.bitrock.dvs.streams.CommonSpecUtils._
import it.bitrock.dvs.streams.TestValues
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.testcommons.Suite
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaConfig
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import org.apache.kafka.common.serialization.Serde
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike

class FlightReceivedListComputationStatusStreamsSpec
    extends Suite
    with AnyWordSpecLike
    with EmbeddedKafkaStreams
    with OptionValues
    with TestValues {

  "FlightReceivedListComputationStatusStreams" should {
    "produce record of FlightReceivedListComputationStatus in delay topic for too old records" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, _) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig                     = embeddedKafkaConfig
        implicit val keySerde: Serde[String]                                 = kafkaStreamsOptions.stringKeySerde
        implicit val flightReceivedListEventSerde: Serde[FlightReceivedList] = kafkaStreamsOptions.flightReceivedListEventSerde
        implicit val valueSerde: Serde[FlightReceivedListComputationStatus] =
          kafkaStreamsOptions.flightReceivedListComputationStatusSerde

        val now = Instant.now()

        val firstMessage = FlightReceivedEvent.copy(
          iataNumber = "iata1",
          icaoNumber = "icao1",
          updated = now.minus(1, ChronoUnit.SECONDS)
        )

        val secondMessage = FlightReceivedEvent.copy(
          iataNumber = "iata2",
          icaoNumber = "icao2",
          updated = now
        )

        val thirdMessage = FlightReceivedEvent.copy(
          iataNumber = "iata3",
          icaoNumber = "icao3",
          updated = now.plus(1, ChronoUnit.SECONDS)
        )

        val receivedList = FlightReceivedList(List(firstMessage, secondMessage, thirdMessage))

        val delayedWindowTime =
          now.minusMillis(appConfig.kafka.monitoring.flightReceivedList.allowedDelay.toMillis + 1000)
        val delayedComputationStatus =
          FlightReceivedListComputationStatus(delayedWindowTime, now, firstMessage.updated, thirdMessage.updated, now, 3)

        val topology = FlightReceivedListComputationStatusStreams.buildTopology(appConfig, kafkaStreamsOptions).map(_._1)
        val receivedRecords =
          ResourceLoaner.runAll(topology) { _ =>
            publishToKafka(
              appConfig.kafka.topology.flightReceivedListTopic.name,
              delayedWindowTime.toEpochMilli.toString,
              receivedList
            )

            val computationStatusMessagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedListComputationStatus](
              topics = Set(appConfig.kafka.monitoring.flightReceivedList.topic),
              number = 1,
              timeout = ConsumerPollTimeout
            )

            val delayMessagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedListComputationStatus](
              topics = Set(appConfig.kafka.monitoring.flightReceivedList.delayTopic),
              number = 1,
              timeout = ConsumerPollTimeout
            )

            (
              computationStatusMessagesMap(appConfig.kafka.monitoring.flightReceivedList.topic).map(_._2),
              delayMessagesMap(appConfig.kafka.monitoring.flightReceivedList.delayTopic).map(_._2)
            )
          }

        val (computationStatus, delay) = receivedRecords

        computationStatus should have size 1
        val cs = computationStatus.head
        cs.minUpdated shouldBe firstMessage.updated
        cs.maxUpdated shouldBe thirdMessage.updated
        cs.averageUpdated shouldBe now
        cs.windowElements shouldBe 3

        delay should have size 1
        delay.head.windowTime shouldBe delayedComputationStatus.windowTime
        delay.head.minUpdated shouldBe delayedComputationStatus.minUpdated
        delay.head.maxUpdated shouldBe delayedComputationStatus.maxUpdated
        delay.head.averageUpdated shouldBe delayedComputationStatus.averageUpdated
        delay.head.windowElements shouldBe delayedComputationStatus.windowElements
    }
  }
}
