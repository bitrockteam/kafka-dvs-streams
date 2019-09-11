package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model._
import it.bitrock.kafkaflightstream.streams.config.AppConfig
import it.bitrock.kafkageostream.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.kafkageostream.testcommons.{FixtureLoanerAnyResult, Suite}
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import net.manub.embeddedkafka.schemaregistry.{EmbeddedKafkaConfig, serdeFrom}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.{StreamsConfig, Topology}
import org.scalatest.{OptionValues, WordSpecLike}

import scala.concurrent.duration._

object StreamsSpec {

  final case class Resource(
      embeddedKafkaConfig: EmbeddedKafkaConfig,
      appConfig: AppConfig,
      kafkaStreamsOptions: KafkaStreamsOptions,
      topology: Topology,
      topicsToCreate: List[String]
  )

}

class StreamsSpec extends Suite with WordSpecLike with EmbeddedKafkaStreams with OptionValues with Events {

  import StreamsSpec._

  final val TopologyTestExtraConf = Map(
    // The commit interval for flushing records to state stores and downstream must be lower than
    // test's timeout (5 secs) to ensure we observe the expected processing results.
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> 3.seconds.toMillis.toString
  )
  final val ConsumerPollTimeout: FiniteDuration = 15.seconds

  "Streams" should {

    "joined succesfully with consistent data" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]    = kafkaStreamsOptions.flightRawSerde

        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val cityRawSerde: Serde[CityRaw]         = kafkaStreamsOptions.cityRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde

        implicit val flightWithDepartureAirportInfo: Serde[FlightWithDepartureAirportInfo] =
          kafkaStreamsOptions.flightWithDepartureAirportInfo
        implicit val flightWithAllAirportInfo: Serde[FlightWithAllAirportInfo] = kafkaStreamsOptions.flightWithAllAirportInfo
        implicit val flightWithAirline: Serde[FlightWithAirline]               = kafkaStreamsOptions.flightWithAirline
        //output topic
        implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent] = kafkaStreamsOptions.flightEnrichedEventSerde
        implicit val topAggregationKeySerde: Serde[Long]                  = kafkaStreamsOptions.topAggregationKeySerde
        implicit val topAirportListSerde: Serde[TopAirportList]           = kafkaStreamsOptions.topAirportListEventSerde
        implicit val topAirportSerde: Serde[Airport]                      = kafkaStreamsOptions.topAirportEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val eventFlight   = EuropeanFlightEvent
          val eventAirport1 = EuropeanAirport1
          val eventAirport2 = EuropeanAirport2
          val eventAirline  = AirlineEvent
          val eventAirplane = AirplaneEvent

          val flightMessage = List(eventFlight.flight.icaoNumber -> eventFlight)
          val airportMessages = List(
            eventAirport1.codeIataAirport -> eventAirport1,
            eventAirport2.codeIataAirport -> eventAirport2
          )
          val airlineMessage  = List(eventAirline.codeIcaoAirline                      -> eventAirline)
          val airplaneMessage = List(eventAirplane.numberRegistration.replace("-", "") -> eventAirplane)

          publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessage)
          publishToKafka(appConfig.kafka.topology.airportRawTopic, airportMessages)
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, airlineMessage)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, airplaneMessage)

          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightEnrichedEvent](
            Set(appConfig.kafka.topology.flightReceivedTopic),
            1,
            // Use greater-than-default timeout since 5 seconds is not enough for the async processing to complete
            timeout = ConsumerPollTimeout
          )

          messagesMap(appConfig.kafka.topology.flightReceivedTopic).take(1)
        }

        val expectedEvent1: FlightEnrichedEvent = ExpectedEuropeanFlightEnrichedEvent

        val expectedResult = List(
          (EuropeanFlightEvent.flight.icaoNumber, expectedEvent1)
        )

        receivedRecords should contain theSameElementsAs expectedResult

      }

    }

    "joined succesfully with consistent data but without airplane informations" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]    = kafkaStreamsOptions.flightRawSerde

        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val cityRawSerde: Serde[CityRaw]         = kafkaStreamsOptions.cityRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde

        implicit val flightWithDepartureAirportInfo: Serde[FlightWithDepartureAirportInfo] =
          kafkaStreamsOptions.flightWithDepartureAirportInfo
        implicit val flightWithAllAirportInfo: Serde[FlightWithAllAirportInfo] = kafkaStreamsOptions.flightWithAllAirportInfo
        implicit val flightWithAirline: Serde[FlightWithAirline]               = kafkaStreamsOptions.flightWithAirline
        //output topic
        implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent] = kafkaStreamsOptions.flightEnrichedEventSerde
        implicit val topAggregationKeySerde: Serde[Long]                  = kafkaStreamsOptions.topAggregationKeySerde
        implicit val topAirportListSerde: Serde[TopAirportList]           = kafkaStreamsOptions.topAirportListEventSerde
        implicit val topAirportSerde: Serde[Airport]                      = kafkaStreamsOptions.topAirportEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val eventFlight   = EuropeanFlightEvent
          val eventAirport1 = EuropeanAirport1
          val eventAirport2 = EuropeanAirport2
          val eventAirline  = AirlineEvent
          val eventAirplane = AirplaneEvent.copy(numberRegistration = "falso")

          val flightMessage = List(eventFlight.flight.icaoNumber -> eventFlight)
          val airportMessages = List(
            eventAirport1.codeIataAirport -> eventAirport1,
            eventAirport2.codeIataAirport -> eventAirport2
          )
          val airlineMessage  = List(eventAirline.codeIcaoAirline                      -> eventAirline)
          val airplaneMessage = List(eventAirplane.numberRegistration.replace("-", "") -> eventAirplane)

          publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessage)
          publishToKafka(appConfig.kafka.topology.airportRawTopic, airportMessages)
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, airlineMessage)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, airplaneMessage)

          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightEnrichedEvent](
            Set(appConfig.kafka.topology.flightReceivedTopic),
            1,
            // Use greater-than-default timeout since 5 seconds is not enough for the async processing to complete
            timeout = ConsumerPollTimeout
          )

          messagesMap(appConfig.kafka.topology.flightReceivedTopic).take(1)
        }
        println(receivedRecords)
        val expectedEvent1: FlightEnrichedEvent = ExpectedFlightEnrichedEventWithoutAirplaneinfo

        val expectedResult = List(
          (EuropeanFlightEvent.flight.icaoNumber, expectedEvent1)
        )

        receivedRecords should contain theSameElementsAs expectedResult

      }
    }

    "joined but without results because arrival airport is outside europe countries list" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig = embeddedKafkaConfig
        implicit val keySerde: Serde[String]             = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]    = kafkaStreamsOptions.flightRawSerde

        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val cityRawSerde: Serde[CityRaw]         = kafkaStreamsOptions.cityRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde

        implicit val flightWithDepartureAirportInfo: Serde[FlightWithDepartureAirportInfo] =
          kafkaStreamsOptions.flightWithDepartureAirportInfo
        implicit val flightWithAllAirportInfo: Serde[FlightWithAllAirportInfo] = kafkaStreamsOptions.flightWithAllAirportInfo
        implicit val flightWithAirline: Serde[FlightWithAirline]               = kafkaStreamsOptions.flightWithAirline
        //output topic
        implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent] = kafkaStreamsOptions.flightEnrichedEventSerde
        implicit val topAggregationKeySerde: Serde[Long]                  = kafkaStreamsOptions.topAggregationKeySerde
        implicit val topAirportListSerde: Serde[TopAirportList]           = kafkaStreamsOptions.topAirportListEventSerde
        implicit val topAirportSerde: Serde[Airport]                      = kafkaStreamsOptions.topAirportEventSerde

        val receivedRecordsSize = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val eventFlight   = ForeignFlightEvent
          val eventAirport1 = EuropeanAirport1
          val eventAirport2 = ForeignAirport1
          val eventAirline  = AirlineEvent
          val eventAirplane = AirplaneEvent

          val flightMessage = List(eventFlight.flight.icaoNumber -> eventFlight)
          val airportMessages = List(
            eventAirport1.codeIataAirport -> eventAirport1,
            eventAirport2.codeIataAirport -> eventAirport2
          )
          val airlineMessage  = List(eventAirline.codeIcaoAirline                      -> eventAirline)
          val airplaneMessage = List(eventAirplane.numberRegistration.replace("-", "") -> eventAirplane)

          publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessage)
          publishToKafka(appConfig.kafka.topology.airportRawTopic, airportMessages)
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, airlineMessage)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, airplaneMessage)

          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightEnrichedEvent](
            Set(appConfig.kafka.topology.flightReceivedTopic),
            0,
            // Use greater-than-default timeout since 5 seconds is not enough for the async processing to complete
            timeout = ConsumerPollTimeout
          )

          messagesMap(appConfig.kafka.topology.flightReceivedTopic).size
        }

        receivedRecordsSize shouldBe 0
      }

    }
  }

  object ResourceLoaner extends FixtureLoanerAnyResult[Resource] {
    override def withFixture(body: Resource => Any): Any = {
      implicit lazy val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

      val appConfig: AppConfig = {
        val conf         = AppConfig.load
        val topologyConf = conf.kafka.topology.copy(aggregationTimeWindowSize = 5.seconds)
        conf.copy(kafka = conf.kafka.copy(topology = topologyConf))
      }

      val kafkaStreamsOptions = KafkaStreamsOptions(
        Serdes.String,
        serdeFrom[FlightRaw],
        serdeFrom[AirportRaw],
        serdeFrom[AirlineRaw],
        serdeFrom[CityRaw],
        serdeFrom[AirplaneRaw],
        serdeFrom[FlightWithDepartureAirportInfo],
        serdeFrom[FlightWithAllAirportInfo],
        serdeFrom[FlightWithAirline],
        serdeFrom[FlightEnrichedEvent],
        Serdes.Long,
        serdeFrom[TopAirportList],
        serdeFrom[Airport]
      )
      val topology = Streams.buildTopology(appConfig, kafkaStreamsOptions)

      val topicsToCreate = List(
        appConfig.kafka.topology.flightRawTopic,
        appConfig.kafka.topology.airlineRawTopic,
        appConfig.kafka.topology.airportRawTopic,
        appConfig.kafka.topology.cityRawTopic,
        appConfig.kafka.topology.airplaneRawTopic,
        appConfig.kafka.topology.flightReceivedTopic
      )

      body(
        Resource(
          embeddedKafkaConfig,
          appConfig,
          kafkaStreamsOptions,
          topology,
          topicsToCreate
        )
      )
    }
  }

}
