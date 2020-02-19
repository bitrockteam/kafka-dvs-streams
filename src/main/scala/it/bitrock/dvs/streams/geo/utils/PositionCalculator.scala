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

  override def position(latitude: Double, longitude: Double, altitude: Double, distance: Double, direction: Double): Position =
    if (distance == 0d) Position(latitude, longitude) else calcPosition(latitude, longitude, altitude, distance, direction)

  private def calcPosition(
      latitude: Double,
      longitude: Double,
      altitude: Double,
      distance: Double,
      direction: Double
  ): Position = {
    val roundTripDistance = (2d * PI * (EarthRadius + altitude))
    val distanceRatio     = (distance % roundTripDistance) / (EarthRadius + altitude)
    val distanceSin       = sin(distanceRatio)
    val distanceCos       = cos(distanceRatio)

    val startLatitudeRad  = degrees2radians(latitude)
    val startLongitudeRad = degrees2radians(longitude)
    val angleRadHeading   = degrees2radians(adjustDirection(direction))

    val startLatitudeCos = cos(startLatitudeRad)
    val startLatitudeSin = sin(startLatitudeRad)

    val endLatitudeRad = asin(startLatitudeSin * distanceCos + startLatitudeCos * distanceSin * cos(angleRadHeading))
    val endLongitudeRad = startLongitudeRad + atan2(
      sin(angleRadHeading) * distanceSin * startLatitudeCos,
      distanceCos - startLatitudeSin * sin(endLatitudeRad)
    )

    Position(
      latitudeCut(radians2degrees(endLatitudeRad)),
      longitudeCut(radians2degrees(endLongitudeRad))
    )
  }

  private def adjustDirection(angle: Double): Double = {
    val angleFloor = (angle + 360) % 360d
    if (angleFloor > 0) angleFloor else 360d - angleFloor
  }

  private def circleCut(deg: Double): Double =
    deg % 360d

  private def longitudeCut(longitude: Double): Double = {
    val deg = circleCut(longitude)
    if (deg > 0) {
      if (deg > 180) 360 - deg else deg
    } else {
      if (deg < -180) -360 - deg else deg
    }
  }

  private def latitudeCut(latitude: Double): Double = {
    val deg = circleCut(latitude)
    if (deg > 0) {
      if (deg > 90) 180 - deg else deg
    } else {
      if (deg < -90) -180 - deg else deg
    }
  }

}
