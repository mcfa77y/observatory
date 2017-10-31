package observatory

import org.scalatest.FunSuite

trait ExtractionTest extends FunSuite {

  //  test("locateTemperatures"){
  //    val base_dir = "src/main/resources/"
  //    val stations_filename = base_dir + "stations_test.csv"
  //    val year = 1975
  //    val temperature_filename = base_dir + year + "_test.csv"
  //    val foo = Extraction.locateTemperatures(year, stations_filename, temperature_filename).toList
  //    assert(foo.head._3 == -40d)
  //    val bar = Extraction.locationYearlyAverageRecords(foo)
  //    assert(bar.head._2 == 0)
  //  }


  test("stations with no location are ignored"){
    val base_dir = "src/main/resources/"
    val year = 1975
    val temperature_filename = base_dir + year + "_test.csv"
    val stations_filename = base_dir + "stations_empty_test.csv"
    val foo = Extraction.locateTemperatures(year, stations_filename, temperature_filename).toList
    assert(foo.size == 1)

  }



}