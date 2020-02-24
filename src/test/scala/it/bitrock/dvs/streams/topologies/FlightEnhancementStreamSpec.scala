package it.bitrock.dvs.streams.topologies

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

class FlightEnhancementStreamSpec extends Suite with AnyWordSpecLike with EmbeddedKafkaStreams with OptionValues with TestValues {
  "FlightEnhancementStream" should {
    "enhance flight raw with updated info" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val icaoNumber = FlightRawEvent.flight.icaoNumber
        val updatedFlight = FlightStateRaw(
          icaoNumber,
          FlightRawEvent.system.updated.plusSeconds(5),
          Geography(123.45, 678.9, 10580.99, 33.5),
          1018.7
        )

        val (key, flight) = ResourceLoaner.runAll(topologies(FlightEnhancementTopology)) { _ =>
          publishToKafka(appConfig.kafka.topology.flightOpenSkyRawTopic.name, icaoNumber, updatedFlight)
          publishToKafka(appConfig.kafka.topology.flightRawTopic.name, icaoNumber, FlightRawEvent)

          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightRaw](
            topics = Set(appConfig.kafka.topology.enhancedFlightRawTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.enhancedFlightRawTopic.name).head
        }

        key shouldBe icaoNumber
        flight.geography shouldBe updatedFlight.geography
        flight.speed.horizontal shouldBe updatedFlight.horizontalSpeed
        flight.system.updated shouldBe updatedFlight.updated
    }

    "not enhance flight raw when no updated info" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val icaoNumber = FlightRawEvent.flight.icaoNumber
        val notUpdatedFlight = FlightStateRaw(
          icaoNumber,
          FlightRawEvent.system.updated.minusSeconds(5),
          Geography(123.45, 678.9, 10580.99, 33.5),
          1018.7
        )

        val (key, flight) = ResourceLoaner.runAll(topologies(FlightEnhancementTopology)) { _ =>
          publishToKafka(appConfig.kafka.topology.flightOpenSkyRawTopic.name, icaoNumber, notUpdatedFlight)
          publishToKafka(appConfig.kafka.topology.flightRawTopic.name, icaoNumber, FlightRawEvent)

          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightRaw](
            topics = Set(appConfig.kafka.topology.enhancedFlightRawTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.enhancedFlightRawTopic.name).head
        }

        key shouldBe icaoNumber
        flight shouldBe FlightRawEvent
    }

    "enhance flight raw with new updated info" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val icaoNumber = FlightRawEvent.flight.icaoNumber
        val updatedFlight = FlightStateRaw(
          icaoNumber,
          FlightRawEvent.system.updated.plusSeconds(5),
          Geography(123.45, 678.9, 10580.99, 33.5),
          1018.7
        )

        val secondUpdatedFlight = FlightStateRaw(
          icaoNumber,
          FlightRawEvent.system.updated.plusSeconds(15),
          Geography(-3.45, -78.9, 6580.99, 133.5),
          483.21
        )

        val ((firstKey, firstFlight), (secondKey, secondFlight)) = ResourceLoaner.runAll(topologies(FlightEnhancementTopology)) {
          _ =>
            publishToKafka(appConfig.kafka.topology.flightOpenSkyRawTopic.name, icaoNumber, updatedFlight)
            publishToKafka(appConfig.kafka.topology.flightRawTopic.name, icaoNumber, FlightRawEvent)

            val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightRaw](
              topics = Set(appConfig.kafka.topology.enhancedFlightRawTopic.name),
              number = 1,
              timeout = ConsumerPollTimeout
            )
            val firstMessage = messagesMap(appConfig.kafka.topology.enhancedFlightRawTopic.name).head

            publishToKafka(appConfig.kafka.topology.flightOpenSkyRawTopic.name, icaoNumber, secondUpdatedFlight)
            publishToKafka(
              appConfig.kafka.topology.flightRawTopic.name,
              icaoNumber,
              FlightRawEvent.copy(system = System(FlightRawEvent.system.updated.plusSeconds(10)))
            )

            val secondMessagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightRaw](
              topics = Set(appConfig.kafka.topology.enhancedFlightRawTopic.name),
              number = 1,
              timeout = ConsumerPollTimeout
            )
            val secondMessage = secondMessagesMap(appConfig.kafka.topology.enhancedFlightRawTopic.name).head

            (firstMessage, secondMessage)
        }

        firstKey shouldBe icaoNumber
        firstFlight.geography shouldBe updatedFlight.geography
        firstFlight.speed.horizontal shouldBe updatedFlight.horizontalSpeed
        firstFlight.system.updated shouldBe updatedFlight.updated

        secondKey shouldBe icaoNumber
        secondFlight.geography shouldBe secondUpdatedFlight.geography
        secondFlight.speed.horizontal shouldBe secondUpdatedFlight.horizontalSpeed
        secondFlight.system.updated shouldBe secondUpdatedFlight.updated
    }

  }
}
