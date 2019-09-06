package it.bitrock.kafkaflightstream.streams

import it.bitrock.kafkaflightstream.model._
import it.bitrock.kafkageostream.kafkacommons.serialization.ImplicitConversions._
import it.bitrock.kafkaflightstream.streams.config.AppConfig
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

class StreamsSpec extends Suite  with WordSpecLike with EmbeddedKafkaStreams with OptionValues with Events {

  import StreamsSpec._

  final val TopologyTestExtraConf = Map(
    // The commit interval for flushing records to state stores and downstream must be lower than
    // test's timeout (5 secs) to ensure we observe the expected processing results.
    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> 3.seconds.toMillis.toString
  )
  final val ConsumerPollTimeout: FiniteDuration = 10.seconds

  //scrivere 1 messaggio nel vari topic
  //e controllare che il messaggio letto e joinato sia quello atteso

  "Streams" should {

    "" in ResourceLoaner.withFixture {
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

        implicit val flightEnrichedEventSerde: Serde[FlightEnrichedEvent] = kafkaStreamsOptions.flightEnrichedEventSerde

        val receivedRecords = runStreams(topicsToCreate, topology, TopologyTestExtraConf) {
          val eventFlight   = FlightEvent
          val eventAirport1 = AirportEvent1
          val eventAirport2 = AirportEvent2
          val eventAirline  = AirlineEvent
          val eventAirplane = AirplaneEvent

          val flightMessage = List(eventFlight.flight.icaoNumber -> eventFlight)
          val airportMessages = List(
            eventAirport1.codeIataAirport -> eventAirport1,
            eventAirport2.codeIataAirport -> eventAirport2
          )
          val airlineMessage = List(eventAirline.codeIcaoAirline -> eventAirline)
          val airplaneMessage = List(eventAirplane.numberRegistration.replace("-","") -> eventAirplane)

          //publishToKafka(appConfig.kafka.topology.flightRawTopic, flightMessage)
          publishToKafka(appConfig.kafka.topology.airportRawTopic, airportMessages)
          //publishToKafka(appConfig.kafka.topology.airlineRawTopic, airlineMessage)
          //publishToKafka(appConfig.kafka.topology.airplaneRawTopic, airplaneMessage)

         val messagesMap = consumeNumberKeyedMessagesFromTopics[String, AirportRaw](
           Set(appConfig.kafka.topology.airportRawTopic),
           2,
           // Use greater-than-default timeout since 5 seconds is not enough for the async processing to complete
           timeout = ConsumerPollTimeout
         )

          messagesMap(appConfig.kafka.topology.airportRawTopic).take(2)
        }

        println(receivedRecords)
       /* val expectedEvent1 = RSVPReceived(
          DefaultEventName,
          DefaultEventLat,
          DefaultEventLon,
          DefaultEventTime,
          MeetupEvent(DefaultEventUrl),
          MeetupUser(DefaultMemberName),
          MeetupGroup(DefaultGroupName, DefaultEventCity, DefaultEventCountry),
          RSVPResponse.YES
        )
        val expectedEvent2 = RSVPReceived(
          DefaultEvent2Name,
          DefaultEvent2Lat,
          DefaultEvent2Lon,
          DefaultEvent2Time,
          MeetupEvent(DefaultEvent2Url),
          MeetupUser(DefaultMember2Name),
          MeetupGroup(DefaultGroup2Name, DefaultEvent2City, DefaultEvent2Country),
          RSVPResponse.NO
        )

        val expectedResult = List(
          (DefaultEventKey, expectedEvent1),
          (DefaultEvent2Key, expectedEvent2)
        )

        receivedRecords should contain theSameElementsInOrderAs expectedResult
*/
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
        serdeFrom[FlightEnrichedEvent]
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
