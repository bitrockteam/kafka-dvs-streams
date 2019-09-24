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
import java.util.concurrent.TimeoutException

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

    "be joined successfully with consistent data" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig  = embeddedKafkaConfig
        implicit val keySerde: Serde[String]              = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
        //output topic
        implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent] = kafkaStreamsOptions.flightEnrichedEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          publishToKafka(appConfig.kafka.topology.flightRawTopic, EuropeanFlightEvent.flight.icaoNumber, EuropeanFlightEvent)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              EuropeanAirport1.codeIataAirport -> EuropeanAirport1,
              EuropeanAirport2.codeIataAirport -> EuropeanAirport2
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightEnrichedEvent](
            topics = Set(appConfig.kafka.topology.flightReceivedTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.flightReceivedTopic).head
        }
        receivedRecords shouldBe (ExpectedEuropeanFlightEnrichedEvent.icaoNumber, ExpectedEuropeanFlightEnrichedEvent)

      }
    }

    "be joined successfully with consistent data but without airplane informations" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig  = embeddedKafkaConfig
        implicit val keySerde: Serde[String]              = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
        //output topic
        implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent] = kafkaStreamsOptions.flightEnrichedEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          publishToKafka(
            appConfig.kafka.topology.flightRawTopic,
            EuropeanFlightEventWithoutAirplane.flight.icaoNumber,
            EuropeanFlightEventWithoutAirplane
          )
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              EuropeanAirport1.codeIataAirport -> EuropeanAirport1,
              EuropeanAirport2.codeIataAirport -> EuropeanAirport2
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, FlightEnrichedEvent](
            topics = Set(appConfig.kafka.topology.flightReceivedTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.flightReceivedTopic).head
        }
        receivedRecords shouldBe (ExpectedEuropeanFlightEnrichedEventWithoutAirplane.icaoNumber, ExpectedEuropeanFlightEnrichedEventWithoutAirplane)

      }
    }

    "be joined but without results because arrival airport is outside europe countries list" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig  = embeddedKafkaConfig
        implicit val keySerde: Serde[String]              = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
        //output topic
        implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent] = kafkaStreamsOptions.flightEnrichedEventSerde

        runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          publishToKafka(appConfig.kafka.topology.flightRawTopic, ForeignFlightEvent.flight.icaoNumber, ForeignFlightEvent)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              EuropeanAirport1.codeIataAirport -> EuropeanAirport1,
              EuropeanAirport2.codeIataAirport -> EuropeanAirport2
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)

          an[TimeoutException] should be thrownBy {
            consumeNumberKeyedMessagesFromTopics[String, FlightEnrichedEvent](
              topics = Set(appConfig.kafka.topology.flightReceivedTopic),
              number = 1,
              timeout = ConsumerPollTimeout
            )
          }
        }

      }
    }

    "produce TopArrivalAirportList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig  = embeddedKafkaConfig
        implicit val keySerde: Serde[String]              = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
        //output topic
        implicit val topArrivalAirportListSerde: Serde[TopArrivalAirportList] = kafkaStreamsOptions.topArrivalAirportListEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val flightMessages = 1 to 40 map { key =>
            val codeIataAirport = key match {
              case x if x >= 1 && x <= 3   => EuropeanAirport1.codeIataAirport
              case x if x >= 4 && x <= 9   => EuropeanAirport2.codeIataAirport
              case x if x >= 10 && x <= 18 => EuropeanAirport3.codeIataAirport
              case x if x >= 19 && x <= 20 => EuropeanAirport4.codeIataAirport
              case x if x >= 21 && x <= 24 => EuropeanAirport5.codeIataAirport
              case x if x >= 25 && x <= 29 => EuropeanAirport6.codeIataAirport
              case x if x >= 30 && x <= 40 => EuropeanAirport7.codeIataAirport
            }
            key.toString -> EuropeanFlightEvent.copy(
              flight = Flight(key.toString, key.toString, ""),
              arrival = CommonCode(codeIataAirport, "")
            )
          }
          publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessages)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              EuropeanAirport1.codeIataAirport -> EuropeanAirport1,
              EuropeanAirport2.codeIataAirport -> EuropeanAirport2,
              EuropeanAirport3.codeIataAirport -> EuropeanAirport3,
              EuropeanAirport4.codeIataAirport -> EuropeanAirport4,
              EuropeanAirport5.codeIataAirport -> EuropeanAirport5,
              EuropeanAirport6.codeIataAirport -> EuropeanAirport6,
              EuropeanAirport7.codeIataAirport -> EuropeanAirport7
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, TopArrivalAirportList](
            topics = Set(appConfig.kafka.topology.topArrivalAirportTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.topArrivalAirportTopic).head._2
        }
        receivedRecords.elements.size shouldBe 5
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedTopArrivalResult.elements

      }
    }

    "produce TopDepartureAirportList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig  = embeddedKafkaConfig
        implicit val keySerde: Serde[String]              = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
        //output topic
        implicit val topDepartureAirportListSerde: Serde[TopDepartureAirportList] = kafkaStreamsOptions.topDepartureAirportListEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val flightMessages = 1 to 40 map { key =>
            val codeIataAirport = key match {
              case x if x >= 1 && x <= 3   => EuropeanAirport1.codeIataAirport
              case x if x >= 4 && x <= 9   => EuropeanAirport2.codeIataAirport
              case x if x >= 10 && x <= 18 => EuropeanAirport3.codeIataAirport
              case x if x >= 19 && x <= 20 => EuropeanAirport4.codeIataAirport
              case x if x >= 21 && x <= 24 => EuropeanAirport5.codeIataAirport
              case x if x >= 25 && x <= 29 => EuropeanAirport6.codeIataAirport
              case x if x >= 30 && x <= 40 => EuropeanAirport7.codeIataAirport
            }
            key.toString -> EuropeanFlightEvent.copy(
              flight = Flight(key.toString, key.toString, ""),
              departure = CommonCode(codeIataAirport, "")
            )
          }
          publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessages)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              EuropeanAirport1.codeIataAirport -> EuropeanAirport1,
              EuropeanAirport2.codeIataAirport -> EuropeanAirport2,
              EuropeanAirport3.codeIataAirport -> EuropeanAirport3,
              EuropeanAirport4.codeIataAirport -> EuropeanAirport4,
              EuropeanAirport5.codeIataAirport -> EuropeanAirport5,
              EuropeanAirport6.codeIataAirport -> EuropeanAirport6,
              EuropeanAirport7.codeIataAirport -> EuropeanAirport7
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, TopDepartureAirportList](
            topics = Set(appConfig.kafka.topology.topDepartureAirportTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.topDepartureAirportTopic).head._2
        }
        receivedRecords.elements.size shouldBe 5
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedTopDepartureResult.elements

      }
    }

    "produce TopSpeedList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig  = embeddedKafkaConfig
        implicit val keySerde: Serde[String]              = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
        //output topic
        implicit val topSpeedListSerde: Serde[TopSpeedList] = kafkaStreamsOptions.topSpeedListEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val flightMessages = 0 to 9 map { key =>
            key.toString -> EuropeanFlightEvent.copy(
              flight = Flight(key.toString, key.toString, ""),
              speed = Speed(SpeedArray(key), 0.0)
            )
          }
          publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessages)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              EuropeanAirport1.codeIataAirport -> EuropeanAirport1,
              EuropeanAirport2.codeIataAirport -> EuropeanAirport2
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, TopSpeedList](
            topics = Set(appConfig.kafka.topology.topSpeedTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.topSpeedTopic).head._2
        }
        receivedRecords.elements.size shouldBe 5
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedTopSpeedResult.elements

      }
    }

    "produce TopAirlineList elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig  = embeddedKafkaConfig
        implicit val keySerde: Serde[String]              = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
        //output topic
        implicit val topAirlineListSerde: Serde[TopAirlineList] = kafkaStreamsOptions.topAirlineListEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val flightMessages = 1 to 40 map { key =>
            val codeAirline = key match {
              case x if x >= 1 && x <= 3   => AirlineEvent1.codeIcaoAirline
              case x if x >= 4 && x <= 9   => AirlineEvent2.codeIcaoAirline
              case x if x >= 10 && x <= 18 => AirlineEvent3.codeIcaoAirline
              case x if x >= 19 && x <= 20 => AirlineEvent4.codeIcaoAirline
              case x if x >= 21 && x <= 24 => AirlineEvent5.codeIcaoAirline
              case x if x >= 25 && x <= 29 => AirlineEvent6.codeIcaoAirline
              case x if x >= 30 && x <= 40 => AirlineEvent7.codeIcaoAirline
            }
            key.toString -> EuropeanFlightEvent.copy(
              flight = Flight(key.toString, key.toString, ""),
              airline = CommonCode("", codeAirline)
            )
          }
          publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessages)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              EuropeanAirport1.codeIataAirport -> EuropeanAirport1,
              EuropeanAirport2.codeIataAirport -> EuropeanAirport2
            )
          )
          publishToKafka(
            appConfig.kafka.topology.airlineRawTopic,
            List(
              AirlineEvent1.codeIcaoAirline -> AirlineEvent1,
              AirlineEvent2.codeIcaoAirline -> AirlineEvent2,
              AirlineEvent3.codeIcaoAirline -> AirlineEvent3,
              AirlineEvent4.codeIcaoAirline -> AirlineEvent4,
              AirlineEvent5.codeIcaoAirline -> AirlineEvent5,
              AirlineEvent6.codeIcaoAirline -> AirlineEvent6,
              AirlineEvent7.codeIcaoAirline -> AirlineEvent7
            )
          )
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, TopAirlineList](
            topics = Set(appConfig.kafka.topology.topAirlineTopic),
            number = 1,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.topAirlineTopic).head._2
        }
        receivedRecords.elements.size shouldBe 5
        receivedRecords.elements should contain theSameElementsInOrderAs ExpectedTopAirlineResult.elements

      }
    }

    "produce TotalFlight elements in the appropriate topic" in ResourceLoaner.withFixture {
      case Resource(embeddedKafkaConfig, appConfig, kafkaStreamsOptions, topology, topicsToCreate) => {
        implicit val embKafkaConfig: EmbeddedKafkaConfig  = embeddedKafkaConfig
        implicit val keySerde: Serde[String]              = kafkaStreamsOptions.keySerde
        implicit val flightRawSerde: Serde[FlightRaw]     = kafkaStreamsOptions.flightRawSerde
        implicit val airportRawSerde: Serde[AirportRaw]   = kafkaStreamsOptions.airportRawSerde
        implicit val airlineRawSerde: Serde[AirlineRaw]   = kafkaStreamsOptions.airlineRawSerde
        implicit val airplaneRawSerde: Serde[AirplaneRaw] = kafkaStreamsOptions.airplaneRawSerde
        //output topic
        implicit val countFlightStatusSerde: Serde[CountFlightStatus] = kafkaStreamsOptions.countFlightStatusEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val flightMessages = 0 to 9 map { key =>
            key.toString -> EuropeanFlightEvent.copy(
              flight = Flight(key.toString, key.toString, ""),
              status = StatusArray(key)
            )
          }
          publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessages)
          publishToKafka(
            appConfig.kafka.topology.airportRawTopic,
            List(
              EuropeanAirport1.codeIataAirport -> EuropeanAirport1,
              EuropeanAirport2.codeIataAirport -> EuropeanAirport2
            )
          )
          publishToKafka(appConfig.kafka.topology.airlineRawTopic, AirlineEvent1.codeIcaoAirline, AirlineEvent1)
          publishToKafka(appConfig.kafka.topology.airplaneRawTopic, AirplaneEvent.numberRegistration, AirplaneEvent)
          val messagesMap = consumeNumberKeyedMessagesFromTopics[String, CountFlightStatus](
            topics = Set(appConfig.kafka.topology.totalFlightTopic),
            number = 3,
            timeout = ConsumerPollTimeout
          )
          messagesMap(appConfig.kafka.topology.totalFlightTopic).map(_._2)
        }
        receivedRecords should contain theSameElementsAs ExpectedTotalFlightResult

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
        serdeFrom[TopArrivalAirportList],
        serdeFrom[TopDepartureAirportList],
        serdeFrom[Airport],
        serdeFrom[TopSpeedList],
        serdeFrom[SpeedFlight],
        serdeFrom[TopAirlineList],
        serdeFrom[Airline],
        serdeFrom[CountFlightStatus]
      )
      val topology = Streams.buildTopology(appConfig, kafkaStreamsOptions)

      body(
        Resource(
          embeddedKafkaConfig,
          appConfig,
          kafkaStreamsOptions,
          topology,
          List()
        )
      )
    }
  }

}
