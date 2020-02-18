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
          updated = now,
          geography = GeographyInfo(40.1d, 9.1d, 0, 0),
          arrivalAirport = AirportInfo(
            ParamsAirport1.iataCode,
            ParamsAirport1.name,
            40.1005d,
            9.1005d,
            "",
            ParamsAirport1.codeCountry,
            "",
            ""
          )
        )

        val key3 = "c"
        val thirdMessage = key3 -> FlightReceivedEvent.copy(
          iataNumber = key3,
          icaoNumber = key3,
          updated = now.plus(1, ChronoUnit.SECONDS)
        )

        val key4 = "d"
        val fourthMessage = key4 -> FlightReceivedEvent.copy(
          iataNumber = key4,
          icaoNumber = key4,
          updated = now,
          geography = GeographyInfo(40.1d, 9.1d, 0, 0),
          departureAirport = AirportInfo(
            ParamsAirport1.iataCode,
            ParamsAirport1.name,
            40.1005d,
            9.1005d,
            "",
            ParamsAirport1.codeCountry,
            "",
            ""
          )
        )

        val flightMessages = List(firstMessage, secondMessage, thirdMessage, fourthMessage)

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

          val landedFlightsMessagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedList](
            topics = Set(appConfig.kafka.topology.flightParkedListTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )

          val enRouteFlightsMessagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedList](
            topics = Set(appConfig.kafka.topology.flightEnRouteListTopic.name),
            number = 1,
            timeout = ConsumerPollTimeout
          )

          (
            messagesMap(appConfig.kafka.topology.flightReceivedListTopic.name).map(_._2),
            landedFlightsMessagesMap(appConfig.kafka.topology.flightParkedListTopic.name).map(_._2),
            enRouteFlightsMessagesMap(appConfig.kafka.topology.flightEnRouteListTopic.name).map(_._2)
          )
        }

        val (flightsReceived, parkedFlights, enRouteFlights) = receivedRecords

        flightsReceived should have size 1
        flightsReceived.head.elements should contain theSameElementsAs flightMessages.map(_._2)

        parkedFlights should have size 1
        parkedFlights.head.elements should contain theSameElementsAs List(secondMessage, fourthMessage).map(_._2)

        enRouteFlights should have size 1
        enRouteFlights.head.elements should contain theSameElementsAs List(firstMessage, thirdMessage).map(_._2)
    }
  }
}
