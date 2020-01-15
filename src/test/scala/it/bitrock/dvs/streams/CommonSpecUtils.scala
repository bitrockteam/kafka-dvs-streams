package it.bitrock.dvs.streams

import java.util.Properties

import it.bitrock.dvs.model.avro._
import it.bitrock.dvs.streams.config.AppConfig
import it.bitrock.dvs.streams.topologies._
import it.bitrock.testcommons.FixtureLoanerAnyResult
import net.manub.embeddedkafka.UUIDs
import net.manub.embeddedkafka.schemaregistry.streams.EmbeddedKafkaStreams
import net.manub.embeddedkafka.schemaregistry.{specificAvroValueSerde, EmbeddedKafkaConfig}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.Serdes

import scala.concurrent.duration._

object CommonSpecUtils {

  final val FlightReceivedTopology              = 1
  final val FlightListTopology                  = 2
  final val TopsTopologies                      = 3
  final val TotalTopologies                     = 4
  final val ConsumerPollTimeout: FiniteDuration = 20.seconds

  final case class Resource(
      embeddedKafkaConfig: EmbeddedKafkaConfig,
      appConfig: AppConfig,
      kafkaStreamsOptions: KafkaStreamsOptions,
      topologies: Map[Int, List[Topology]]
  )

  object ResourceLoaner extends FixtureLoanerAnyResult[Resource] with EmbeddedKafkaStreams {

    override def withFixture(body: Resource => Any): Any = {

      implicit lazy val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig()

      val config: AppConfig = {
        val conf         = AppConfig.load
        val topologyConf = conf.kafka.topology.copy(aggregationTimeWindowSize = 5.seconds, aggregationTotalTimeWindowSize = 5.seconds)
        conf.copy(kafka = conf.kafka.copy(topology = topologyConf))
      }

      val kafkaStreamsOptions = KafkaStreamsOptions(
        Serdes.String,
        specificAvroValueSerde[FlightRaw],
        specificAvroValueSerde[AirportRaw],
        specificAvroValueSerde[AirlineRaw],
        specificAvroValueSerde[CityRaw],
        specificAvroValueSerde[AirplaneRaw],
        specificAvroValueSerde[FlightWithDepartureAirportInfo],
        specificAvroValueSerde[FlightWithAllAirportInfo],
        specificAvroValueSerde[FlightWithAirline],
        specificAvroValueSerde[FlightReceived],
        specificAvroValueSerde[FlightReceivedList],
        Serdes.Long,
        specificAvroValueSerde[TopArrivalAirportList],
        specificAvroValueSerde[TopDepartureAirportList],
        specificAvroValueSerde[Airport],
        specificAvroValueSerde[TopSpeedList],
        specificAvroValueSerde[SpeedFlight],
        specificAvroValueSerde[TopAirlineList],
        specificAvroValueSerde[Airline],
        specificAvroValueSerde[CountFlight],
        specificAvroValueSerde[CountAirline],
        specificAvroValueSerde[CodeAirlineList],
        specificAvroValueSerde[FlightNumberList]
      )

      val topologies = Map(
        (FlightReceivedTopology, FlightReceivedStream.buildTopology(config, kafkaStreamsOptions).map(_._1)),
        (FlightListTopology, FlightListStream.buildTopology(config, kafkaStreamsOptions).map(_._1)),
        (TopsTopologies, TopStreams.buildTopology(config, kafkaStreamsOptions).map(_._1)),
        (TotalTopologies, TotalStreams.buildTopology(config, kafkaStreamsOptions).map(_._1))
      )

      body(
        Resource(
          embeddedKafkaConfig,
          config,
          kafkaStreamsOptions,
          topologies
        )
      )
    }

    def runAll[A](topologies: List[Topology])(body: List[KafkaStreams] => A): A = {
      val TopologyTestExtraConf = Map(
        // The commit interval for flushing records to state stores and downstream must be lower than
        // test's timeout (5 secs) to ensure we observe the expected processing results.
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> 3.seconds.toMillis.toString
      )
      runStreams(Nil, topologies.head, TopologyTestExtraConf) {
        import scala.collection.JavaConverters._
        val streams = topologies.tail.map(topology => {
          val streamsConf = streamsConfig.config(UUIDs.newUuid().toString, TopologyTestExtraConf)
          val props       = new Properties
          props.putAll(streamsConf.asJava)
          val otherStream = new KafkaStreams(topology, props)
          otherStream.start()
          otherStream
        })
        val result = body(streams)
        streams.foreach(_.close())
        result
      }
    }

  }

}
