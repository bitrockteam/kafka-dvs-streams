/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package it.bitrock.dvs.model.avro.monitoring

import scala.annotation.switch

case class FlightReceivedListComputationStatus(
    var windowTime: java.time.Instant,
    var emissionTime: java.time.Instant,
    var minUpdated: java.time.Instant,
    var maxUpdated: java.time.Instant
) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this(java.time.Instant.now, java.time.Instant.now, java.time.Instant.now, java.time.Instant.now)
  def get(field$ : Int): AnyRef =
    (field$ : @switch) match {
      case 0 => {
        windowTime.toEpochMilli
      }.asInstanceOf[AnyRef]
      case 1 => {
        emissionTime.toEpochMilli
      }.asInstanceOf[AnyRef]
      case 2 => {
        minUpdated.toEpochMilli
      }.asInstanceOf[AnyRef]
      case 3 => {
        maxUpdated.toEpochMilli
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  def put(field$ : Int, value: Any): Unit = {
    (field$ : @switch) match {
      case 0 =>
        this.windowTime = {
          value match {
            case (l: Long) => {
              java.time.Instant.ofEpochMilli(l)
            }
          }
        }.asInstanceOf[java.time.Instant]
      case 1 =>
        this.emissionTime = {
          value match {
            case (l: Long) => {
              java.time.Instant.ofEpochMilli(l)
            }
          }
        }.asInstanceOf[java.time.Instant]
      case 2 =>
        this.minUpdated = {
          value match {
            case (l: Long) => {
              java.time.Instant.ofEpochMilli(l)
            }
          }
        }.asInstanceOf[java.time.Instant]
      case 3 =>
        this.maxUpdated = {
          value match {
            case (l: Long) => {
              java.time.Instant.ofEpochMilli(l)
            }
          }
        }.asInstanceOf[java.time.Instant]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = FlightReceivedListComputationStatus.SCHEMA$
}

object FlightReceivedListComputationStatus {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"FlightReceivedListComputationStatus\",\"namespace\":\"it.bitrock.dvs.model.avro.monitoring\",\"fields\":[{\"name\":\"windowTime\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"emissionTime\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"minUpdated\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"maxUpdated\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}]}"
  )
}
