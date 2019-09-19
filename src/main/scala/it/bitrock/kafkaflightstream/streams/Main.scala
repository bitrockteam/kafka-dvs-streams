package it.bitrock.kafkaflightstream.streams

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.LazyLogging
import it.bitrock.kafkaflightstream.model.{
  Airline,
  AirlineRaw,
  AirplaneRaw,
  Airport,
  AirportRaw,
  CityRaw,
  CountFlightStatus,
  FlightEnrichedEvent,
  FlightRaw,
  FlightWithAirline,
  FlightWithAllAirportInfo,
  FlightWithDepartureAirportInfo,
  SpeedFlight,
  TopAirlineList,
  TopArrivalAirportList,
  TopDepartureAirportList,
  TopSpeedList
}
import it.bitrock.kafkaflightstream.streams.config.AppConfig
import it.bitrock.kafkageostream.kafkacommons.serialization.AvroSerdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.Serdes

import scala.concurrent.duration._

object Main extends App with LazyLogging {
  logger.info("Starting up")

  val config = AppConfig.load
  logger.debug(s"Loaded configuration: $config")

  val avroSerdes = new AvroSerdes(config.kafka.schemaRegistryUrl)

  val kafkaStreamsOptions = KafkaStreamsOptions(
    Serdes.String,
    avroSerdes.serdeFrom[FlightRaw],
    avroSerdes.serdeFrom[AirportRaw],
    avroSerdes.serdeFrom[AirlineRaw],
    avroSerdes.serdeFrom[CityRaw],
    avroSerdes.serdeFrom[AirplaneRaw],
    avroSerdes.serdeFrom[FlightWithDepartureAirportInfo],
    avroSerdes.serdeFrom[FlightWithAllAirportInfo],
    avroSerdes.serdeFrom[FlightWithAirline],
    avroSerdes.serdeFrom[FlightEnrichedEvent],
    Serdes.Long,
    avroSerdes.serdeFrom[TopArrivalAirportList],
    avroSerdes.serdeFrom[TopDepartureAirportList],
    avroSerdes.serdeFrom[Airport],
    avroSerdes.serdeFrom[TopSpeedList],
    avroSerdes.serdeFrom[SpeedFlight],
    avroSerdes.serdeFrom[TopAirlineList],
    avroSerdes.serdeFrom[Airline],
    avroSerdes.serdeFrom[CountFlightStatus]
  )

  val topology = Streams.buildTopology(config, kafkaStreamsOptions)
  logger.debug(s"Built topology: ${topology.describe}")

  val kafkaStreamsProperties = Streams.streamProperties(config.kafka)
  logger.debug(s"Using streams properties: $kafkaStreamsProperties")

  val streams = new KafkaStreams(topology, kafkaStreamsProperties)
  val latch   = new CountDownLatch(1)

  sys.addShutdownHook {
    logger.info("Shutting down")

    if (streams.state.isRunning) {
      val shutdownTimeout = 1.second
      streams.close(duration2JavaDuration(shutdownTimeout))
    }

    latch.countDown()
  }

  streams.setUncaughtExceptionHandler((_: Thread, e: Throwable) => {
    logger.error("Uncaught exception while running streams", e)
    System.exit(0)
  })

  try {
    logger.info("Starting streams")
    streams.start()
    latch.await()
  } catch {
    case e: Throwable =>
      logger.error("Exception starting streams", e)
      System.exit(1)
  }

}
