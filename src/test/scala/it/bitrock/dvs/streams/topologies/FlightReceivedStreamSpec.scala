package it.bitrock.dvs.streams.topologies

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams.CommonSpecUtils._
import it.bitrock.dvs.streams.TestValues
import it.bitrock.testcommons.Suite
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import net.manub.embeddedkafka.schemaregistry._
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import org.apache.kafka.common.serialization.Serde
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike

class FlightReceivedStreamSpec extends Suite with AnyWordSpecLike with EmbeddedKafkaStreams with OptionValues with TestValues {

  "FlightReceivedStream" should {

    "be joined successfully with consistent data" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.keySerde

        val receivedRecords = ResourceLoaner.runAll(topologies(FlightReceivedTopology)) { _ =>
          publishToKafka(appConfig.kafka.topology.flightRawTopic, FlightRawEvent.flight.icaoNumber, FlightRawEvent)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              AirportEvent1.codeIataAirport -> AirportEvent1,
              AirportEvent2.codeIataAirport -> AirportEvent2
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceived](
            topics = Set(appConfig.kafka.topology.flightReceivedTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.flightReceivedTopic).head
        }
        receivedRecords shouldBe ((FlightReceivedEvent.icaoNumber, FlightReceivedEvent))
    }

    "be joined successfully with default airplane info" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.keySerde

        val receivedRecords = ResourceLoaner.runAll(topologies(FlightReceivedTopology)) { _ =>
          publishToKafka(appConfig.kafka.topology.flightRawTopic, FlightRawEvent.flight.icaoNumber, FlightRawEvent)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              AirportEvent1.codeIataAirport -> AirportEvent1,
              AirportEvent2.codeIataAirport -> AirportEvent2
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceived](
            topics = Set(appConfig.kafka.topology.flightReceivedTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.flightReceivedTopic).head
        }
        receivedRecords shouldBe (
          (
            ExpectedFlightReceivedWithDefaultAirplane.icaoNumber,
            ExpectedFlightReceivedWithDefaultAirplane
          )
        )
    }

  }

}
