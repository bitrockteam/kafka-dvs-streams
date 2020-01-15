package it.bitrock.dvs.streams.config

import pureconfig.generic.auto._

final case class AppConfig(
    kafka: KafkaConfig,
    topElementsAmount: Int
)

object AppConfig {

  def load: AppConfig = pureconfig.loadConfigOrThrow[AppConfig]

}
