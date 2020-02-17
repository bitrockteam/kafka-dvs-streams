package it.bitrock.dvs.streams.geo.utils

import org.scalatest.ParallelTestExecution
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import EarthPositionCalculatorSpec._

class EarthPositionCalculatorSpec extends AnyWordSpecLike with Matchers with ParallelTestExecution {

  "EarthPositionCalculator" should {

    "return the same position when distance is zero" in {
      EarthPositionCalculator.position(0d, 0d, 0d, 17) shouldBe Position(0d, 0d)
    }

    "return the opposite Position with half world trip" in {
      val newPosition = EarthPositionCalculator.position(0d, 0d, EarthHalfRound, 180)

      newPosition.longitude should equal(180d +- 1)
      newPosition.latitude should equal(0d +- 1)
    }

    "return the Position with the same longitude with world trip" in {
      val newPosition = EarthPositionCalculator.position(0d, 0d, EarthRound, 180)

      newPosition.longitude should equal(0d +- 2)
      newPosition.latitude should equal(0d +- 2)
    }

    "return the Position with quart trip of 45 deg" in {
      val newPosition = EarthPositionCalculator.position(0d, 0d, EarthHalfRound / 2, 45)

      newPosition.longitude should equal(90d +- 1)
      newPosition.latitude should equal(45d +- 1)
    }

  }

}

object EarthPositionCalculatorSpec {
  val EarthHalfRound: Double = 6371e3d * Math.PI
  val EarthRound: Double     = 2 * EarthHalfRound
}
