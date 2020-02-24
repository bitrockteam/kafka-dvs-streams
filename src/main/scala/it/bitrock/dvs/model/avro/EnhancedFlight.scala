/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package it.bitrock.dvs.model.avro

import scala.annotation.switch

case class EnhancedFlight(
    var icaoNumber: String,
    var updated: java.time.Instant,
    var geography: Geography,
    var horizontalSpeed: Double
) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("", java.time.Instant.now, new Geography, 0.0)
  def get(field$ : Int): AnyRef =
    (field$ : @switch) match {
      case 0 => {
          icaoNumber
        }.asInstanceOf[AnyRef]
      case 1 => {
          updated.toEpochMilli
        }.asInstanceOf[AnyRef]
      case 2 => {
          geography
        }.asInstanceOf[AnyRef]
      case 3 => {
          horizontalSpeed
        }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  def put(field$ : Int, value: Any): Unit = {
    (field$ : @switch) match {
      case 0 =>
        this.icaoNumber = {
          value.toString
        }.asInstanceOf[String]
      case 1 =>
        this.updated = {
          value match {
            case (l: Long) => {
              java.time.Instant.ofEpochMilli(l)
            }
          }
        }.asInstanceOf[java.time.Instant]
      case 2 =>
        this.geography = {
          value
        }.asInstanceOf[Geography]
      case 3 =>
        this.horizontalSpeed = {
          value
        }.asInstanceOf[Double]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = EnhancedFlight.SCHEMA$
}

object EnhancedFlight {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"EnhancedFlight\",\"namespace\":\"it.bitrock.dvs.model.avro\",\"fields\":[{\"name\":\"icaoNumber\",\"type\":\"string\"},{\"name\":\"updated\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"geography\",\"type\":{\"type\":\"record\",\"name\":\"Geography\",\"fields\":[{\"name\":\"latitude\",\"type\":\"double\"},{\"name\":\"longitude\",\"type\":\"double\"},{\"name\":\"altitude\",\"type\":\"double\"},{\"name\":\"direction\",\"type\":\"double\"}]}},{\"name\":\"horizontalSpeed\",\"type\":\"double\"}]}"
  )
}
