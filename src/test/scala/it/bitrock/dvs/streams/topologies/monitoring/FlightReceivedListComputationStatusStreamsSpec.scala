package it.bitrock.dvs.streams.topologies.monitoring

import java.time.Instant

import it.bitrock.dvs.model.avro.monitoring.FlightReceivedListComputationStatus
import it.bitrock.dvs.streams.CommonSpecUtils._
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
    with OptionValues {

  "FlightReceivedListComputationStatusStreams" should {

    "produce record in delay topic for too old records" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, _) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.keySerde
        implicit val valueSerde: Serde[FlightReceivedListComputationStatus] =
          kafkaStreamsOptions.flightReceivedListComputationStatusSerde

        val now = Instant.now()
        val delayedWindowTime =
          now.minusMillis(appConfig.kafka.monitoring.flightReceivedList.allowedDelay.toMillis + 1000)
        val delayedComputationStatus = FlightReceivedListComputationStatus(delayedWindowTime, now, now, now)

        val topology = FlightReceivedListComputationStatusStreams.buildTopology(appConfig, kafkaStreamsOptions).map(_._1)
        val receivedRecords =
          ResourceLoaner.runAll(topology) { _ =>
            publishToKafka(
              appConfig.kafka.monitoring.flightReceivedList.topic,
              "unused",
              FlightReceivedListComputationStatus(now, now, now, now)
            )

            publishToKafka(appConfig.kafka.monitoring.flightReceivedList.topic, "unused", delayedComputationStatus)

            val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedListComputationStatus](
              topics = Set(appConfig.kafka.monitoring.flightReceivedList.delayTopic),
              number = 1,
              timeout = ConsumerPollTimeout
            )

            messagesMap(appConfig.kafka.monitoring.flightReceivedList.delayTopic).map(_._2)
          }

        receivedRecords should have size 1
        receivedRecords.head shouldBe delayedComputationStatus
    }

  }

}
