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

class FlightReceivedStreamSpec extends Suite with AnyWordSpecLike with EmbeddedKafkaStreams with OptionValues with TestValues {
  "FlightReceivedStream" should {
    "be joined successfully with consistent data" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val zurich        = "Zurich"
        val milan         = "Milan"
        val cityIataCode1 = "CIC1"
        val cityIataCode2 = "CIC2"

        val (airportsInfo, receivedRecords) =
          ResourceLoaner.runAll(
            topologies(FlightReceivedTopology),
            List(
              appConfig.kafka.topology.flightRawTopic.name,
              appConfig.kafka.topology.airplaneRawTopic.name,
              appConfig.kafka.topology.enhancedFlightRawTopic.name,
              appConfig.kafka.topology.airlineRawTopic.name
            )
          ) { _ =>
            publishToKafka(
              appConfig.kafka.topology.airportRawTopic.name,
              List(
                AirportEvent1.iataCode -> AirportEvent1.copy(cityIataCode = cityIataCode1),
                AirportEvent2.iataCode -> AirportEvent2.copy(cityIataCode = cityIataCode2)
              )
            )
            publishToKafka(
              appConfig.kafka.topology.cityRawTopic.name,
              List(
                cityIataCode1 -> CityRaw(1L, zurich, cityIataCode1, "", 0d, 0d),
                cityIataCode2 -> CityRaw(2L, milan, cityIataCode2, "", 0d, 0d)
              )
            )
            publishToKafka(appConfig.kafka.topology.airlineRawTopic.name, AirlineEvent1.icaoCode, AirlineEvent1)
            publishToKafka(appConfig.kafka.topology.airplaneRawTopic.name, AirplaneEvent.registrationNumber, AirplaneEvent)

            val airportInfoMap = consumeNumberKeyedMessagesFromTopics[String, AirportInfo](
              topics = Set(appConfig.kafka.topology.airportInfoTopic.name),
              number = 2,
              timeout = ConsumerPollTimeout
            )

            publishToKafka(appConfig.kafka.topology.enhancedFlightRawTopic.name, FlightRawEvent.flight.icaoNumber, FlightRawEvent)

            val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceived](
              topics = Set(appConfig.kafka.topology.flightReceivedTopic.name),
              number = 1,
              timeout = ConsumerPollTimeout
            )

            (
              airportInfoMap(appConfig.kafka.topology.airportInfoTopic.name),
              messagesMap(appConfig.kafka.topology.flightReceivedTopic.name).head
            )

          }

        val departureAirportInfo = FlightReceivedEvent.departureAirport.copy(city = zurich)
        val arrivalAirportInfo   = FlightReceivedEvent.arrivalAirport.copy(city = milan)

        airportsInfo should contain theSameElementsAs List(
          AirportEvent1.iataCode -> departureAirportInfo,
          AirportEvent2.iataCode -> arrivalAirportInfo
        )

        receivedRecords shouldBe (
          (
            FlightReceivedEvent.icaoNumber,
            FlightReceivedEvent.copy(departureAirport = departureAirportInfo, arrivalAirport = arrivalAirportInfo)
          )
        )
    }

    "be joined successfully with default airplane info and city info" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topologies) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val (airportsInfo, receivedRecords) =
          ResourceLoaner.runAll(
            topologies(FlightReceivedTopology),
            List(
              appConfig.kafka.topology.flightRawTopic.name,
              appConfig.kafka.topology.airplaneRawTopic.name,
              appConfig.kafka.topology.enhancedFlightRawTopic.name,
              appConfig.kafka.topology.airlineRawTopic.name
            )
          ) { _ =>
            publishToKafka(
              appConfig.kafka.topology.airportRawTopic.name,
              List(
                AirportEvent1.iataCode -> AirportEvent1,
                AirportEvent2.iataCode -> AirportEvent2
              )
            )
            publishToKafka(appConfig.kafka.topology.airlineRawTopic.name, AirlineEvent1.icaoCode, AirlineEvent1)
            val airportInfoMap = consumeNumberKeyedMessagesFromTopics[String, AirportInfo](
              topics = Set(appConfig.kafka.topology.airportInfoTopic.name),
              number = 2,
              timeout = ConsumerPollTimeout
            )

            publishToKafka(appConfig.kafka.topology.enhancedFlightRawTopic.name, FlightRawEvent.flight.icaoNumber, FlightRawEvent)
            val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightReceived](
              topics = Set(appConfig.kafka.topology.flightReceivedTopic.name),
              number = 1,
              timeout = ConsumerPollTimeout
            )
            (
              airportInfoMap(appConfig.kafka.topology.airportInfoTopic.name),
              messagesMap(appConfig.kafka.topology.flightReceivedTopic.name).head
            )
          }

        val departureAirportInfo = FlightReceivedEvent.departureAirport.copy(city = FlightReceivedStream.defaultMissingValue)
        val arrivalAirportInfo   = FlightReceivedEvent.arrivalAirport.copy(city = FlightReceivedStream.defaultMissingValue)

        airportsInfo should contain theSameElementsAs List(
          AirportEvent1.iataCode -> departureAirportInfo,
          AirportEvent2.iataCode -> arrivalAirportInfo
        )

        receivedRecords shouldBe (
          (
            ExpectedFlightReceivedWithDefaultAirplane.icaoNumber,
            ExpectedFlightReceivedWithDefaultAirplane.copy(
              arrivalAirport = arrivalAirportInfo,
              departureAirport = departureAirportInfo
            )
          )
        )
    }
  }
}
