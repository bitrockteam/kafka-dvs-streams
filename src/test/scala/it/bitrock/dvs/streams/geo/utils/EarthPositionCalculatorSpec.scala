package it.bitrock.dvs.streams.geo.utils

import org.scalatest.ParallelTestExecution
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import EarthPositionCalculatorSpec._

class EarthPositionCalculatorSpec extends AnyWordSpecLike with Matchers with ParallelTestExecution {

  "EarthPositionCalculator" should {

    "return the same position when distance is zero" in {
      EarthPositionCalculator.position(0d, 0d, 0d, 0d, 17) shouldBe Position(0d, 0d)
    }

    "return the opposite Position with half world trip with zero altitude" in {
      val newPosition = EarthPositionCalculator.position(0d, 0d, 0d, earthHalfRound(0), 180 + 360)

      newPosition.longitude should equal(180d +- 1)
      newPosition.latitude should equal(0d +- 1)
    }

    "return the opposite Position with half world trip with non zero altitude" in {
      val altitude    = 10000d
      val newPosition = EarthPositionCalculator.position(0d, 0d, altitude, earthHalfRound(altitude), 180 + 360)

      newPosition.longitude should equal(180d +- 1)
      newPosition.latitude should equal(0d +- 1)
    }

    "return the Position with the same longitude with world trip" in {
      val newPosition = EarthPositionCalculator.position(0d, 0d, 0d, earthRound(0), -180)

      newPosition.longitude should equal(0d +- 2)
      newPosition.latitude should equal(0d +- 2)
    }

    "return the Position with quart trip of 45 deg" in {
      val newPosition = EarthPositionCalculator.position(0d, 0d, 0d, earthHalfRound(0) / 2, 45)

      newPosition.longitude should equal(90d +- 1)
      newPosition.latitude should equal(45d +- 1)
    }

  }

}

object EarthPositionCalculatorSpec {
  def earthHalfRound(altitude: Double): Double = (6371e3d + altitude) * Math.PI
  def earthRound(altitude: Double): Double     = 2 * earthHalfRound(altitude)
}
