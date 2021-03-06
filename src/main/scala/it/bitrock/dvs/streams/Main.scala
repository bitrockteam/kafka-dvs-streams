package it.bitrock.dvs.streams

import java.time.Clock
import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.LazyLogging
import it.bitrock.dvs.model.avro.monitoring.FlightReceivedListComputationStatus
import it.bitrock.dvs.model.avro.{System => _, _}
import it.bitrock.dvs.streams.config.AppConfig
import it.bitrock.dvs.streams.topologies._
import it.bitrock.dvs.streams.topologies.monitoring.FlightReceivedListComputationStatusStreams
import it.bitrock.kafkacommons.serialization.AvroSerdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.Serdes

import scala.concurrent.duration._
import scala.util.control.NonFatal

object Main extends App with LazyLogging {
  logger.info("Starting up")
  implicit private val clock: Clock = Clock.systemUTC()

  val config = AppConfig.load
  logger.debug(s"Loaded configuration: $config")

  val avroSerdes = new AvroSerdes(config.kafka.schemaRegistryUrl)

  val kafkaStreamsOptions = KafkaStreamsOptions(
    Serdes.String,
    Serdes.Integer,
    avroSerdes.serdeFrom[FlightRaw],
    avroSerdes.serdeFrom[FlightStateRaw],
    avroSerdes.serdeFrom[AirportRaw],
    avroSerdes.serdeFrom[AirlineRaw],
    avroSerdes.serdeFrom[CityRaw],
    avroSerdes.serdeFrom[AirplaneRaw],
    avroSerdes.serdeFrom[AirportInfo],
    avroSerdes.serdeFrom[FlightWithDepartureAirportInfo],
    avroSerdes.serdeFrom[FlightWithAllAirportInfo],
    avroSerdes.serdeFrom[FlightWithAirline],
    avroSerdes.serdeFrom[FlightReceived],
    avroSerdes.serdeFrom[FlightReceivedList],
    avroSerdes.serdeFrom[FlightInterpolatedList],
    Serdes.Long,
    avroSerdes.serdeFrom[TopArrivalAirportList],
    avroSerdes.serdeFrom[TopDepartureAirportList],
    avroSerdes.serdeFrom[TopAirport],
    avroSerdes.serdeFrom[TopSpeedList],
    avroSerdes.serdeFrom[TopSpeed],
    avroSerdes.serdeFrom[TopAirlineList],
    avroSerdes.serdeFrom[TopAirline],
    avroSerdes.serdeFrom[CountFlight],
    avroSerdes.serdeFrom[CountAirline],
    avroSerdes.serdeFrom[CodeAirlineList],
    avroSerdes.serdeFrom[FlightNumberList],
    avroSerdes.serdeFrom[FlightReceivedListComputationStatus]
  )

  val flightReceivedTopology = FlightReceivedStream.buildTopology(config, kafkaStreamsOptions)
  val flightListTopology     = FlightListStream.buildTopology(config, kafkaStreamsOptions)
  val topsTopology           = TopStreams.buildTopology(config, kafkaStreamsOptions)
  val totalsTopology         = TotalStreams.buildTopology(config, kafkaStreamsOptions)
  val flightReceivedListComputationStatusStreamsTopology =
    FlightReceivedListComputationStatusStreams.buildTopology(config, kafkaStreamsOptions)
  val flightInterpolatedListTopology = FlightInterpolatedListStream.buildTopology(config, kafkaStreamsOptions)
  val flightEnhancementTopology      = FlightEnhancementStream.buildTopology(config, kafkaStreamsOptions)

  val topologies = flightReceivedTopology ++
    flightListTopology ++
    topsTopology ++
    totalsTopology ++
    flightReceivedListComputationStatusStreamsTopology ++
    flightInterpolatedListTopology ++
    flightEnhancementTopology

  val streams = topologies.map {
    case (topology, props) =>
      logger.debug(s"Built topology: ${topology.describe}")
      logger.debug(s"Using streams properties: $props")
      new KafkaStreams(topology, props)
  }
  val latch = new CountDownLatch(streams.size)

  streams.foreach { stream =>
    sys.addShutdownHook {
      logger.info("Shutting down")
      if (stream.state.isRunning) {
        val shutdownTimeout = 1.second
        stream.close(duration2JavaDuration(shutdownTimeout))
      }
      latch.countDown()
    }
    stream.setUncaughtExceptionHandler { (_: Thread, e: Throwable) =>
      logger.error("Uncaught exception while running streams", e)
      System.exit(0)
    }
  }

  try {
    logger.info("Starting streams")
    streams.foreach(stream => stream.start())
    latch.await()
  } catch {
    case NonFatal(e) =>
      logger.error("Exception starting streams", e)
      System.exit(1)
  }
}
