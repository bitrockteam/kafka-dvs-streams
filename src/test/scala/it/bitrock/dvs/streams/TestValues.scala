package it.bitrock.dvs.streams

import java.time.Instant

import it.bitrock.dvs.model.avro._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

trait TestValues {
  final val FlightIataCode = "EI35Y"
  final val FlightIcaoCode = "EIN35Y"
  final val Updated        = Instant.now
  final val StatusEnRoute  = "en-route"

  final val Latitude  = 45.0
  final val Longitude = 9.0

  final val ParamsAirport1 = AirportParams("ZRH", "Zurich", "CH")
  final val ParamsAirport2 = AirportParams("MXP", "Malpensa", "IT")
  final val ParamsAirport3 = AirportParams("MXR", "Moussoro", "UA")
  final val ParamsAirport4 = AirportParams("NAP", "Napoli Capodichino", "IT")
  final val ParamsAirport5 = AirportParams("NCE", "Nice", "FR")
  final val ParamsAirport6 = AirportParams("AEI", "Algeciras", "ES")
  final val ParamsAirport7 = AirportParams("BVG", "Berlevåg", "NO")
  final val ParamsAirline1 = AirlineParams("SWR", "SWISS")
  final val ParamsAirline2 = AirlineParams("ESQ", "Europ Star Aircraft")
  final val ParamsAirline3 = AirlineParams("FFI", "Infinit Air")
  final val ParamsAirline4 = AirlineParams("GEC", "Lufthansa Cargo")
  final val ParamsAirline5 = AirlineParams("LLP", "Small Planet Airlines Polska")
  final val ParamsAirline6 = AirlineParams("DRU", "ALROSA Mirny Air Enterprise")
  final val ParamsAirline7 = AirlineParams("KAT", "Kato Airline")
  final val ParamsAirplane = AirplaneParams("HBJHA")
  final val ParamsFlight = FlightParams(
    ParamsAirport1.iataCode,
    ParamsAirport2.iataCode,
    ParamsAirline1.icaoCode,
    ParamsAirplane.numberRegistration
  )

  final val SpeedArray = Array(123.45, 800, 958.37, 1216.67, 750, 987, 675.45, 900, 1000, 345.89)
  final val ProductionLineArray = Array(
    "Boeing 727",
    "McDonnell Douglas DC",
    "Airbus A318/A319/A32",
    "Boeing 737 NG",
    "",
    "Embraer EMB-190/EMB-",
    "ATR 42/72",
    "Saab 340",
    "Airbus A330/A340",
    "Boeing 737 Classic"
  )
  final val CodeAirlineArray = Array(
    ParamsAirline1.icaoCode,
    ParamsAirline2.icaoCode,
    ParamsAirline3.icaoCode,
    ParamsAirline4.icaoCode,
    ParamsAirline1.icaoCode,
    ParamsAirline3.icaoCode,
    ParamsAirline2.icaoCode,
    ParamsAirline5.icaoCode,
    ParamsAirline1.icaoCode,
    ParamsAirline1.icaoCode
  )

  final val AirportEvent1: AirportRaw  = buildAirportRaw(ParamsAirport1)
  final val AirportEvent2: AirportRaw  = buildAirportRaw(ParamsAirport2)
  final val AirportEvent3: AirportRaw  = buildAirportRaw(ParamsAirport3)
  final val AirportEvent4: AirportRaw  = buildAirportRaw(ParamsAirport4)
  final val AirportEvent5: AirportRaw  = buildAirportRaw(ParamsAirport5)
  final val AirportEvent6: AirportRaw  = buildAirportRaw(ParamsAirport6)
  final val AirportEvent7: AirportRaw  = buildAirportRaw(ParamsAirport7)
  final val AirlineEvent1: AirlineRaw  = buildAirlineRaw(ParamsAirline1)
  final val AirlineEvent2: AirlineRaw  = buildAirlineRaw(ParamsAirline2)
  final val AirlineEvent3: AirlineRaw  = buildAirlineRaw(ParamsAirline3)
  final val AirlineEvent4: AirlineRaw  = buildAirlineRaw(ParamsAirline4)
  final val AirlineEvent5: AirlineRaw  = buildAirlineRaw(ParamsAirline5)
  final val AirlineEvent6: AirlineRaw  = buildAirlineRaw(ParamsAirline6)
  final val AirlineEvent7: AirlineRaw  = buildAirlineRaw(ParamsAirline7)
  final val AirplaneEvent: AirplaneRaw = buildAirplaneRaw(ParamsAirplane)
  final val FlightRawEvent: FlightRaw  = buildFlightRaw(ParamsFlight)

  final val FlightReceivedEvent = FlightReceived(
    FlightIataCode,
    FlightIcaoCode,
    GeographyInfo(0, 0, 0, 0),
    0,
    AirportInfo(ParamsAirport1.iataCode, ParamsAirport1.name, Latitude, Longitude, "", ParamsAirport1.codeCountry, "", ""),
    AirportInfo(ParamsAirport2.iataCode, ParamsAirport2.name, Latitude, Longitude, "", ParamsAirport2.codeCountry, "", ""),
    AirlineInfo(ParamsAirline1.icaoCode, ParamsAirline1.nameAirline, 0),
    AirplaneInfo(ParamsAirplane.numberRegistration, "", ""),
    StatusEnRoute,
    Updated
  )
  final val ExpectedFlightReceivedWithDefaultAirplane = FlightReceivedEvent.copy(airplane = AirplaneInfo("N/A", "N/A", "N/A"))
  final val ExpectedTopArrivalResult = TopArrivalAirportList(
    List(
      TopAirport(ParamsAirport7.name, 11),
      TopAirport(ParamsAirport3.name, 9),
      TopAirport(ParamsAirport2.name, 6),
      TopAirport(ParamsAirport6.name, 5),
      TopAirport(ParamsAirport5.name, 4)
    )
  )
  final val ExpectedTopDepartureResult = TopDepartureAirportList(
    List(
      TopAirport(ParamsAirport7.name, 11),
      TopAirport(ParamsAirport3.name, 9),
      TopAirport(ParamsAirport2.name, 6),
      TopAirport(ParamsAirport6.name, 5),
      TopAirport(ParamsAirport5.name, 4)
    )
  )
  final val ExpectedTopSpeedResult = TopSpeedList(
    Seq(
      TopSpeed("3", SpeedArray(3)),
      TopSpeed("8", SpeedArray(8)),
      TopSpeed("5", SpeedArray(5)),
      TopSpeed("2", SpeedArray(2)),
      TopSpeed("7", SpeedArray(7))
    )
  )
  final val ExpectedTopAirlineResult = TopAirlineList(
    Seq(
      TopAirline(ParamsAirline7.nameAirline, 11),
      TopAirline(ParamsAirline3.nameAirline, 9),
      TopAirline(ParamsAirline2.nameAirline, 6),
      TopAirline(ParamsAirline6.nameAirline, 5),
      TopAirline(ParamsAirline5.nameAirline, 4)
    )
  )
  final val ExpectedTotalFlightResult  = 10
  final val ExpectedTotalAirlineResult = 5

  case class AirportParams(iataCode: String, name: String, codeCountry: String)
  case class AirlineParams(icaoCode: String, nameAirline: String)
  case class AirplaneParams(numberRegistration: String)
  case class FlightParams(departureAirportCode: String, arrivalAirportCode: String, airlineCode: String, airplaneCode: String)

  private def buildAirportRaw(params: AirportParams) =
    AirportRaw(0, params.name, params.iataCode, Latitude, Longitude, "", params.codeCountry, "", "", "")
  private def buildAirlineRaw(params: AirlineParams) = AirlineRaw(0, params.nameAirline, "", params.icaoCode, "", "", 0, "", "")
  private def buildAirplaneRaw(params: AirplaneParams) =
    AirplaneRaw(params.numberRegistration, "", "", "", "", "", "", "", "", "", "")
  private def buildFlightRaw(params: FlightParams) =
    FlightRaw(
      Geography(0, 0, 0, 0),
      Speed(0, 0),
      Departure(params.departureAirportCode, None),
      Arrival(params.arrivalAirportCode, None),
      Aircraft(params.airplaneCode, "", "", ""),
      Airline(None, params.airlineCode),
      Flight(FlightIataCode, FlightIcaoCode, ""),
      System(Updated),
      StatusEnRoute
    )

  @SuppressWarnings(Array("DisableSyntax.null"))
  def dummyFlightReceivedForcingSuppression(topic: String, delay: FiniteDuration = 1.minute) = new ProducerRecord(
    topic,
    null,
    java.lang.System.currentTimeMillis + delay.toMillis,
    "",
    FlightReceivedEvent
  )
}
