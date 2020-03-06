package it.bitrock.dvs.streams.topologies

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams.CommonSpecUtils._
import it.bitrock.dvs.streams.TestValues._
import it.bitrock.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.testcommons.Suite
import net.manub.embeddedkafka.schemaregistry._
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import org.apache.kafka.common.serialization.Serde
import org.scalatest.OptionValues
import org.scalatest.wordspec.AnyWordSpecLike

class TopStreamsSpec extends Suite with AnyWordSpecLike with EmbeddedKafkaStreams with OptionValues {
  "TopStreams" should {
    "produce TopArrivalAirportList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val receivedRecords = ResourceLoaner.runAll(topologies(TopsTopologies)) { _ =>
          val flightMessages = 1 to 40 map { key =>
            val airportName = key match {
              case x if x >= 1 && x <= 3   => AirportEvent1.name
              case x if x >= 4 && x <= 9   => AirportEvent2.name
              case x if x >= 10 && x <= 18 => AirportEvent3.name
              case x if x >= 19 && x <= 20 => AirportEvent4.name
              case x if x >= 21 && x <= 24 => AirportEvent5.name
              case x if x >= 25 && x <= 29 => AirportEvent6.name
              case x if x >= 30 && x <= 40 => AirportEvent7.name
            }
            key.toString -> FlightReceivedEvent.copy(
              iataNumber = key.toString,
              icaoNumber = key.toString,
              arrivalAirport = AirportInfo("iataCode1", airportName, Latitude, Longitude, "", "", "", "", "")
            )
          }
          publishToKafka(appConfig.kafka.topology.flightReceivedTopic.name, flightMessages)
          publishToKafka(dummyFlightReceivedForcingSuppression(appConfig.kafka.topology.flightReceivedTopic.name))
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, TopArrivalAirportList](
            topics = Set(appConfig.kafka.topology.topArrivalAirportTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.topArrivalAirportTopic.name).head._2
        }
        receivedRecords.elements.size shouldBe 5
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedTopArrivalResult.elements
    }

    "produce TopDepartureAirportList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val receivedRecords = ResourceLoaner.runAll(topologies(TopsTopologies)) { _ =>
          val flightMessages = 1 to 40 map { key =>
            val airportName = key match {
              case x if x >= 1 && x <= 3   => AirportEvent1.name
              case x if x >= 4 && x <= 9   => AirportEvent2.name
              case x if x >= 10 && x <= 18 => AirportEvent3.name
              case x if x >= 19 && x <= 20 => AirportEvent4.name
              case x if x >= 21 && x <= 24 => AirportEvent5.name
              case x if x >= 25 && x <= 29 => AirportEvent6.name
              case x if x >= 30 && x <= 40 => AirportEvent7.name
            }
            key.toString -> FlightReceivedEvent.copy(
              iataNumber = key.toString,
              icaoNumber = key.toString,
              departureAirport = AirportInfo("iataCode2", airportName, Latitude, Longitude, "", "", "", "", "")
            )
          }
          publishToKafka(appConfig.kafka.topology.flightReceivedTopic.name, flightMessages)
          publishToKafka(dummyFlightReceivedForcingSuppression(appConfig.kafka.topology.flightReceivedTopic.name))
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, TopDepartureAirportList](
            topics = Set(appConfig.kafka.topology.topDepartureAirportTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.topDepartureAirportTopic.name).head._2
        }
        receivedRecords.elements.size shouldBe 5
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedTopDepartureResult.elements
    }

    "produce TopSpeedList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val receivedRecords = ResourceLoaner.runAll(topologies(TopsTopologies)) { _ =>
          val flightMessages = 0 to 9 map { key =>
            key.toString -> FlightReceivedEvent.copy(
              iataNumber = key.toString,
              icaoNumber = key.toString,
              speed = SpeedArray(key)
            )
          }
          publishToKafka(appConfig.kafka.topology.flightReceivedTopic.name, flightMessages)
          publishToKafka(dummyFlightReceivedForcingSuppression(appConfig.kafka.topology.flightReceivedTopic.name))
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, TopSpeedList](
            topics = Set(appConfig.kafka.topology.topSpeedTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.topSpeedTopic.name).head._2
        }
        receivedRecords.elements.size shouldBe 5
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedTopSpeedResult.elements
    }

    "produce TopAirlineList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val receivedRecords = ResourceLoaner.runAll(topologies(TopsTopologies)) { _ =>
          val flightMessages = 1 to 40 map { key =>
            val (codeAirline, nameAirline) = key match {
              case x if x >= 1 && x <= 3   => (AirlineEvent1.icaoCode, AirlineEvent1.name)
              case x if x >= 4 && x <= 9   => (AirlineEvent2.icaoCode, AirlineEvent2.name)
              case x if x >= 10 && x <= 18 => (AirlineEvent3.icaoCode, AirlineEvent3.name)
              case x if x >= 19 && x <= 20 => (AirlineEvent4.icaoCode, AirlineEvent4.name)
              case x if x >= 21 && x <= 24 => (AirlineEvent5.icaoCode, AirlineEvent5.name)
              case x if x >= 25 && x <= 29 => (AirlineEvent6.icaoCode, AirlineEvent6.name)
              case x if x >= 30 && x <= 40 => (AirlineEvent7.icaoCode, AirlineEvent7.name)
            }
            key.toString -> FlightReceivedEvent.copy(
              iataNumber = key.toString,
              icaoNumber = key.toString,
              airline = AirlineInfo(codeAirline, nameAirline, 0)
            )
          }
          publishToKafka(appConfig.kafka.topology.flightReceivedTopic.name, flightMessages)
          publishToKafka(dummyFlightReceivedForcingSuppression(appConfig.kafka.topology.flightReceivedTopic.name))
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, TopAirlineList](
            topics = Set(appConfig.kafka.topology.topAirlineTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.topAirlineTopic.name).head._2
        }
        receivedRecords.elements.size shouldBe 5
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedTopAirlineResult.elements
    }
  }
}
