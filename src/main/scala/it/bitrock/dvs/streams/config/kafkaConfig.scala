package it.bitrock.dvs.streams.config

import java.net.URI

import scala.concurrent.duration.FiniteDuration

final case class KafkaConfig(
    applicationId: String,
    bootstrapServers: String,
    schemaRegistryUrl: URI,
    topology: TopologyConfig,
    enableInterceptors: Boolean
)

final case class TopologyConfig(
    flightRawTopic: String,
    airportRawTopic: String,
    airlineRawTopic: String,
    cityRawTopic: String,
    airplaneRawTopic: String,
    flightReceivedTopic: String,
    flightReceivedListTopic: String,
    topArrivalAirportTopic: String,
    topDepartureAirportTopic: String,
    topSpeedTopic: String,
    topAirlineTopic: String,
    totalFlightTopic: String,
    totalAirlineTopic: String,
    computationStatusTopic: String,
    aggregationTimeWindowSize: FiniteDuration,
    aggregationTotalTimeWindowSize: FiniteDuration,
    aggregationTimeWindowGrace: FiniteDuration,
    commitInterval: FiniteDuration,
    cacheMaxSizeBytes: Long,
    maxRequestSize: Long,
    threadsAmount: Int
)
