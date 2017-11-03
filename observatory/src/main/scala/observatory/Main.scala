package observatory
import observatory.Extraction.{averageRecordsSpark, locateTemperaturesSpark}

object Main extends App {
  val base_dir = "src/main/resources/"
  val stations_filename = base_dir + "stations.csv"
  val year = 1975
  val temperature_filename = base_dir + year + ".csv"

  val foo = Extraction.locateTemperaturesSpark(year, stations_filename, temperature_filename)


  //  val foo = Extraction.locateTemperatures(year, stations_filename, temperature_filename)
  //  val foo = Extraction.locateTemperatures(year, stations_filename, temperature_filename)
  //  val bar = Extraction.locationYearlyAverageRecords(foo)
  val bar = averageRecordsSpark(foo)
  //  println("locate temp size: " + foo.size)
  println("location Year avg rec size: " + bar.count)

}
