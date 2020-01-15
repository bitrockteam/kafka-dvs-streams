package it.bitrock.dvs.streams.topologies

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams.CommonSpecUtils._
import it.bitrock.dvs.streams.TestValues
import it.bitrock.testcommons.Suite
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import net.manub.embeddedkafka.schemaregistry._
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import org.apache.kafka.common.serialization.Serde
import org.scalatest.{OptionValues, WordSpecLike}

class FlightListStreamSpec extends Suite with WordSpecLike with EmbeddedKafkaStreams with OptionValues with TestValues {

  "FlightListStream" should {

    "produce FlightReceivedList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.keySerde

        val receivedRecords = ResourceLoaner.runAll(topologies(FlightListTopology)) { _ =>
          val flightMessages = 0 to 9 map { key =>
            key.toString -> FlightReceivedEvent.copy(
              iataNumber = key.toString,
              icaoNumber = key.toString
            )
          }
          publishToKafka(appConfig.kafka.topology.flightReceivedTopic, flightMessages)
          publishToKafka(dummyFlightReceivedForcingSuppression(appConfig.kafka.topology.flightReceivedTopic))
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedList](
            topics = Set(appConfig.kafka.topology.flightReceivedListTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.flightReceivedListTopic).map(_._2).head
        }
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedFlightReceivedList
    }

  }

}
