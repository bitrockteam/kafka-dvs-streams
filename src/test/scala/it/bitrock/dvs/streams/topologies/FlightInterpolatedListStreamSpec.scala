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
import org.scalatest.{Assertion, OptionValues}
import org.scalatest.wordspec.AnyWordSpecLike

class FlightInterpolatedListStreamSpec
    extends Suite
    with AnyWordSpecLike
    with EmbeddedKafkaStreams
    with OptionValues
    with TestValues {
  "FlightInterpolatedListStream" should {
    "produce interpolated record of FlightReceivedList in interpolated topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, _) =>
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.stringKeySerde

        val expectedMessages = (ConsumerPollTimeout / appConfig.kafka.topology.interpolationInterval).toInt

        val now = Instant.now()

        val firstMessage = FlightReceivedEvent.copy(
          iataNumber = "iata1",
          icaoNumber = "icao1",
          updated = now.minus(1, ChronoUnit.SECONDS),
          geography = GeographyInfo(15d, 30d, 12345d, 13.4),
          speed = 980.9
        )

        val secondMessage = FlightReceivedEvent.copy(
          iataNumber = "iata2",
          icaoNumber = "icao2",
          updated = now,
          geography = GeographyInfo(45d, 10d, 11045d, 73.4),
          speed = 456.77
        )

        val thirdMessage = FlightReceivedEvent.copy(
          iataNumber = "iata3",
          icaoNumber = "icao3",
          updated = now.plus(1, ChronoUnit.SECONDS),
          geography = GeographyInfo(-3.4d, -78.02d, 9165d, -9.2),
          speed = 1052.33
        )

        val receivedList = FlightReceivedList(List(firstMessage, secondMessage, thirdMessage))

        val topology = FlightInterpolatedListStream.buildTopology(appConfig, kafkaStreamsOptions).map(_._1)
        val receivedRecords = ResourceLoaner.runAll(topology) { _ =>
          publishToKafka(
            appConfig.kafka.topology.flightEnRouteListTopic.name,
            now.toEpochMilli.toString,
            receivedList
          )

          val messages = consumeNumberKeyedMessagesFromTopics[String, FlightReceivedList](
            topics = Set(appConfig.kafka.topology.flightInterpolatedListTopic.name),
            number = expectedMessages,
            timeout = ConsumerPollTimeout
          )

          messages(appConfig.kafka.topology.flightInterpolatedListTopic.name)

        }

        val endTestTime = Instant.now.toEpochMilli

        receivedRecords should have size expectedMessages
        receivedRecords.head._2.elements should contain theSameElementsInOrderAs receivedList.elements

        receivedRecords.tail.foreach {
          case (k, v) =>
            k.toLong shouldBe <(endTestTime)
            k.toLong shouldBe >(now.minus(1, ChronoUnit.SECONDS).toEpochMilli)

            val confrontedFlights = v.elements.sortBy(_.icaoNumber) zip receivedList.elements.sortBy(_.icaoNumber)

            confrontedFlights.foreach { case (interpolated, original) => compareFlights(interpolated, original) }
        }

    }

  }

  private def compareFlights(interpolated: FlightReceived, original: FlightReceived): Assertion = {
    interpolated.iataNumber shouldBe original.iataNumber
    interpolated.icaoNumber shouldBe original.icaoNumber

    interpolated.geography should not be original.geography
    interpolated.geography.latitude should not be original.geography.latitude
    interpolated.geography.longitude should not be original.geography.longitude

    interpolated.geography.altitude shouldBe original.geography.altitude
    interpolated.geography.direction shouldBe original.geography.direction
    interpolated.speed shouldBe original.speed
  }
}
