package it.bitrock.dvs.streams.geo.utils

import org.scalatest.ParallelTestExecution
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class EarthPositionCalculatorSpec extends AnyWordSpecLike with Matchers with ParallelTestExecution {

  "EarthPositionCalculator" should {
    "return the same position when round the worlds" in {
      EarthPositionCalculator.position(0d, 0d, (6378d * 2) * Math.PI, 180) shouldBe Position(0d, 0d)
    }
  }
}
