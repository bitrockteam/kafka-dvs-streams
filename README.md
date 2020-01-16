# DVS streams

[![Build Status](https://iproject-jenkins.reactive-labs.io/buildStatus/icon?job=kafka-dvs-streams%2Fmaster)](https://iproject-jenkins.reactive-labs.io/job/kafka-dvs-streams/job/master/)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)

## Configuration

The application references the following environment variables:

- `KAFKA.BOOTSTRAP.SERVERS`: valid `bootstrap.servers` value (see [Confluent docs](https://docs.confluent.io/current/clients/consumer.html#configuration))
- `SCHEMAREGISTRY.URL`: valid `schema.registry.url` value (see [Confluent docs](https://docs.confluent.io/current/schema-registry/docs/schema_registry_tutorial.html#java-consumers))

## Dependencies

### Resolvers

Some dependencies are downloaded from a private Nexus repository. Make sure to provide a `~/.sbt/.credentials.bitrock` file containing valid credentials:

```properties
realm=Sonatype Nexus Repository Manager
host=nexus.reactive-labs.io
user=<your-username>
password=<your-password>
```

### Kafka topics

The application references the following Kafka topics:


## How to test

Execute unit tests running the following command:

```sh
sbt test
```

## How to build

Build and publish Docker image running the following command:

```sh
sbt docker:publish
```

## Architectural diagram
