import sbt._

object Dependencies {
  object CustomResolvers {
    lazy val BitrockNexus = "Bitrock Nexus" at "https://nexus.reactive-labs.io/repository/maven-bitrock-public/"
    lazy val Confluent    = "confluent" at "https://packages.confluent.io/maven/"

    lazy val resolvers: Seq[Resolver] = Seq(BitrockNexus, Confluent)
  }

  object Versions {
    lazy val Scala               = "2.12.10"
    lazy val ConfluentPlatform   = "5.4.4"
    lazy val EmbeddedKafka       = "5.4.1.2"
    lazy val Diffx               = "0.4.5"
    lazy val JakartaWsRs         = "2.1.6"
    lazy val Kafka               = "2.4.1"
    lazy val KafkaCommons        = "0.0.8"
    lazy val KafkaDVSAvroSchemas = "1.0.20"
    lazy val LogbackClassic      = "1.2.4"
    lazy val PureConfig          = "0.12.3"
    lazy val ScalacheckShapeless = "1.2.5"
    lazy val ScalafixSortImports = "0.6.1"
    lazy val ScalaLogging        = "3.9.4"
    lazy val ScalaTestAutofix    = "3.1.0.1"
    lazy val Slf4j               = "1.7.31"
    lazy val TestCommons         = "0.0.8"
    lazy val ScalaTestPlus       = "3.1.1.1"

  }

  object Logging {
    lazy val prodDeps: Seq[ModuleID] = Seq(
      "ch.qos.logback"              % "logback-classic"  % Versions.LogbackClassic, // required by scala-logging
      "com.typesafe.scala-logging" %% "scala-logging"    % Versions.ScalaLogging,
      "org.slf4j"                   % "log4j-over-slf4j" % Versions.Slf4j // mandatory when log4j gets excluded
    )

    lazy val excludeDeps: Seq[ExclusionRule] = Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12"),
      ExclusionRule("log4j")
    )
  }

  lazy val prodDeps: Seq[ModuleID] = Seq(
    "com.github.pureconfig" %% "pureconfig"              % Versions.PureConfig,
    "io.confluent"           % "kafka-avro-serializer"   % Versions.ConfluentPlatform,
    "it.bitrock.dvs"        %% "kafka-dvs-avro-schemas"  % Versions.KafkaDVSAvroSchemas,
    "it.bitrock"            %% "kafka-commons"           % Versions.KafkaCommons,
    "org.apache.kafka"      %% "kafka-streams-scala"     % Versions.Kafka,
    "jakarta.ws.rs"          % "jakarta.ws.rs-api"       % Versions.JakartaWsRs, // mandatory when javax.ws.rs-api gets excluded
    "io.confluent"           % "monitoring-interceptors" % Versions.ConfluentPlatform exclude ("org.apache.kafka", "kafka-clients")
  ) ++ Logging.prodDeps

  lazy val testDeps: Seq[ModuleID] = Seq(
    "com.softwaremill.diffx"     %% "diffx-scalatest"                % Versions.Diffx,
    "io.github.embeddedkafka"    %% "embedded-kafka-schema-registry" % Versions.EmbeddedKafka,
    "it.bitrock"                 %% "test-commons"                   % Versions.TestCommons,
    "org.scalatestplus"          %% "scalacheck-1-14"                % Versions.ScalaTestPlus,
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.14"      % Versions.ScalacheckShapeless
  ).map(_ % Test)

  lazy val excludeDeps: Seq[ExclusionRule] = Seq(
    ExclusionRule("javax.ws.rs", "javax.ws.rs-api")
  ) ++ Logging.excludeDeps
}
