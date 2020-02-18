package it.bitrock.dvs.streams.geo.utils

import java.lang.Math._

trait PositionCalculator {

  /**
    *
    * @param latitude of starting point
    * @param longitude of starting point
    * @param altitude expressed in meters
    * @param distance expressed in meters
    * @param direction in degrees
    * @return a new [[it.bitrock.dvs.streams.geo.utils.Position]]
    */
  def position(latitude: Double, longitude: Double, altitude: Double, distance: Double, direction: Double): Position

}

object EarthPositionCalculator extends PositionCalculator {
  private val EarthRadius: Double = 6371e3d

  private val degrees2radians: Double => Double = _ / 180d * PI
  private val radians2degrees: Double => Double = _ * 180d / PI

  override def position(latitude: Double, longitude: Double, altitude: Double, distance: Double, direction: Double): Position = {
    val distanceRatio = distance / (EarthRadius + altitude)
    val distanceSin   = sin(distanceRatio)
    val distanceCos   = cos(distanceRatio)

    val startLatitudeRad  = degrees2radians(latitude)
    val startLongitudeRad = degrees2radians(longitude)
    val angleRadHeading   = degrees2radians(direction)

    val startLatitudeCos = cos(startLatitudeRad)
    val startLatitudeSin = sin(startLatitudeRad)

    val endLatitudeRad = asin(startLatitudeSin * distanceCos + startLatitudeCos * distanceSin * cos(angleRadHeading))
    val endLongitudeRad = startLongitudeRad + atan2(
      sin(angleRadHeading) * distanceSin * startLatitudeCos,
      distanceCos - startLatitudeSin * sin(endLatitudeRad)
    )

    Position(
      radians2degrees(endLatitudeRad),
      radians2degrees(endLongitudeRad)
    )
  }

}
