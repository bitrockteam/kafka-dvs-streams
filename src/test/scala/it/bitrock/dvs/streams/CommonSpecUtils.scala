package it.bitrock.dvs.streams

import java.util.Properties

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.model.avro.monitoring.FlightReceivedListComputationStatus
import it.bitrock.dvs.streams.config.AppConfig
import it.bitrock.dvs.streams.topologies._
import it.bitrock.testcommons.FixtureLoanerAnyResult
import net.manub.embeddedkafka.UUIDs
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import net.manub.embeddedkafka.schemaregistry.{specificAvroValueSerde, EmbeddedKafkaConfig}
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object CommonSpecUtils {
  sealed trait TopologyType
  case object FlightReceivedTopology    extends TopologyType
  case object FlightListTopology        extends TopologyType
  case object TopsTopologies            extends TopologyType
  case object TotalTopologies           extends TopologyType
  case object FlightEnhancementTopology extends TopologyType
  final val ConsumerPollTimeout: FiniteDuration = 23.seconds

  final case class Resource(
      embeddedKafkaConfig: EmbeddedKafkaConfig,
      appConfig: AppConfig,
      kafkaStreamsOptions: KafkaStreamsOptions,
      topologies: Map[TopologyType, List[Topology]]
  )

  object ResourceLoaner extends FixtureLoanerAnyResult[Resource] with EmbeddedKafkaStreams {
    override def withFixture(body: Resource => Any): Any = {
      implicit lazy val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

      val config: AppConfig = {
        val conf = AppConfig.load
        val topologyConf =
          conf.kafka.topology.copy(
            aggregationTimeWindowSize = 5.seconds,
            aggregationTotalTimeWindowSize = 5.seconds,
            interpolationInterval = 3.seconds
          )
        conf.copy(kafka = conf.kafka.copy(topology = topologyConf, enableInterceptors = false))
      }

      val topologies: Map[TopologyType, List[Topology]] = Map(
        (FlightReceivedTopology, FlightReceivedStream.buildTopology(config, kafkaStreamsSerde).map(_._1)),
        (FlightListTopology, FlightListStream.buildTopology(config, kafkaStreamsSerde).map(_._1)),
        (TopsTopologies, TopStreams.buildTopology(config, kafkaStreamsSerde).map(_._1)),
        (TotalTopologies, TotalStreams.buildTopology(config, kafkaStreamsSerde).map(_._1)),
        (FlightEnhancementTopology, FlightEnhancementStream.buildTopology(config, kafkaStreamsSerde).map(_._1))
      )

      body(
        Resource(
          embeddedKafkaConfig,
          config,
          kafkaStreamsSerde,
          topologies
        )
      )
    }

    private def kafkaStreamsSerde(implicit config: EmbeddedKafkaConfig): KafkaStreamsOptions =
      KafkaStreamsOptions(
        Serdes.String,
        Serdes.Integer,
        specificAvroValueSerde[FlightRaw],
        specificAvroValueSerde[FlightStateRaw],
        specificAvroValueSerde[AirportRaw],
        specificAvroValueSerde[AirlineRaw],
        specificAvroValueSerde[CityRaw],
        specificAvroValueSerde[AirplaneRaw],
        specificAvroValueSerde[AirportInfo],
        specificAvroValueSerde[FlightWithDepartureAirportInfo],
        specificAvroValueSerde[FlightWithAllAirportInfo],
        specificAvroValueSerde[FlightWithAirline],
        specificAvroValueSerde[FlightReceived],
        specificAvroValueSerde[FlightReceivedList],
        specificAvroValueSerde[FlightInterpolatedList],
        Serdes.Long,
        specificAvroValueSerde[TopArrivalAirportList],
        specificAvroValueSerde[TopDepartureAirportList],
        specificAvroValueSerde[TopAirport],
        specificAvroValueSerde[TopSpeedList],
        specificAvroValueSerde[TopSpeed],
        specificAvroValueSerde[TopAirlineList],
        specificAvroValueSerde[TopAirline],
        specificAvroValueSerde[CountFlight],
        specificAvroValueSerde[CountAirline],
        specificAvroValueSerde[CodeAirlineList],
        specificAvroValueSerde[FlightNumberList],
        specificAvroValueSerde[FlightReceivedListComputationStatus]
      )

    def runAll[A](topologies: List[Topology], topicsToCreate: List[String] = List.empty)(body: List[KafkaStreams] => A): A = {
      val topologyTestExtraConf = Map(
        // The commit interval for flushing records to state stores and downstream must be lower than
        // test's timeout (5 secs) to ensure we observe the expected processing results.
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> 3.seconds.toMillis.toString
      )
      runStreams(topicsToCreate, topologies.head, topologyTestExtraConf) {
        val streams = topologies.tail.map { topology =>
          val streamsConf = streamsConfig.config(UUIDs.newUuid().toString, topologyTestExtraConf)
          val props       = new Properties
          props.putAll(streamsConf.asJava)
          val otherStream = new KafkaStreams(topology, props)
          otherStream.start()
          otherStream
        }
        val result = body(streams)
        streams.foreach(_.close())
        result
      }
    }
  }
}
