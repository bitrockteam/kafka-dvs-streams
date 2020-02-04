package it.bitrock.dvs.streams.config

import java.net.URI

import scala.concurrent.duration.FiniteDuration

final case class KafkaConfig(
    applicationId: String,
    bootstrapServers: String,
    schemaRegistryUrl: URI,
    topology: TopologyConfig,
    enableInterceptors: Boolean,
    monitoring: Monitoring
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
    aggregationTimeWindowSize: FiniteDuration,
    aggregationTotalTimeWindowSize: FiniteDuration,
    aggregationTimeWindowGrace: FiniteDuration,
    commitInterval: FiniteDuration,
    cacheMaxSizeBytes: Long,
    maxRequestSize: Long,
    threadsAmount: Int
)

final case class Monitoring(flightReceivedList: MonitoringConf)
final case class MonitoringConf(allowedDelay: FiniteDuration, topic: String, delayTopic: String)
