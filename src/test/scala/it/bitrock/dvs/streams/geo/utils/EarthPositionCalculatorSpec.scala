package it.bitrock.dvs.streams.geo.utils

import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EarthPositionCalculatorSpec extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {

  val latitudeGen: Gen[Double]  = Gen.choose(-90d, 90d) :| "latitude"
  val longitudeGen: Gen[Double] = Gen.choose(-180d, 180d) :| "longitude"
  val altitudeGen: Gen[Double]  = Gen.posNum[Double] :| "altitude"
  val distanceGen: Gen[Double]  = Gen.posNum[Double] :| "distance"
  val directionGen: Gen[Double] = Arbitrary.arbDouble.arbitrary :| "direction"

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(
    minSuccessful = 1000
  )

  def earthRound(altitude: Double): Double             = 2d * Math.PI * (6371e3d + altitude)
  private def earthHalfRound(altitude: Double): Double = earthRound(altitude) / 2d

  implicit val doubleNoShrink: Shrink[Double] = Shrink[Double](_ => Stream.empty)

  property("return the position with latitude in range [-90, 90] and longitude in range [-180, 180]") {
    forAll(latitudeGen, longitudeGen, altitudeGen, distanceGen, directionGen) {
      (latitude: Double, longitude: Double, altitude: Double, distance: Double, direction: Double) =>
        val newPosition = EarthPositionCalculator.position(latitude, longitude, altitude, distance, direction)

        newPosition.latitude shouldBe >=(-90d)
        newPosition.latitude shouldBe <=(90d)

        newPosition.longitude shouldBe >=(-180d)
        newPosition.longitude shouldBe <=(1800d)
    }
  }

  property("return the same position when distance is zero") {
    forAll { (latitude: Double, longitude: Double, altitude: Double, direction: Double) =>
      EarthPositionCalculator.position(latitude, longitude, altitude, 0, direction) shouldBe Position(latitude, longitude)
    }
  }

  property("return the opposite Position with half world trip") {
    forAll(altitudeGen, directionGen) { (altitude: Double, direction: Double) =>
      {
        val newPosition = EarthPositionCalculator.position(0d, 180d, altitude, earthHalfRound(altitude), direction)

        newPosition.latitude should equal(0d +- 1)
        newPosition.longitude should equal(0d +- 1)
      }
    }
  }

  property("return to the same Position on world trip distance") {
    forAll(latitudeGen, longitudeGen, altitudeGen, directionGen) {
      (latitude: Double, longitude: Double, altitude: Double, direction: Double) =>
        {
          require(altitude >= 0)
          val distance = earthRound(altitude)

          val newPosition = EarthPositionCalculator.position(latitude, longitude, altitude, distance, direction)

          newPosition.longitude should equal(longitude +- 2)
          newPosition.latitude should equal(latitude +- 2)
        }
    }
  }

  property("return the Position with quart trip of 45 deg") {
    forAll(altitudeGen) { altitude: Double =>
      {
        val newPosition = EarthPositionCalculator.position(0d, 0d, altitude, earthHalfRound(altitude) / 2, 45)

        newPosition.longitude should equal(90d +- 1)
        newPosition.latitude should equal(45d +- 1)
      }
    }
  }

}
