package observatory
import observatory.Extraction.{sc, sparkAverageRecords, sparkLocateTemperatures}
import org.apache.spark.rdd.RDD

object Main extends App {
  val base_dir = "src/main/resources/"
  val stations_filename = base_dir + "stations.csv"
  val year = 1975
  val temperature_filename = base_dir + year + ".csv"

  val stations: RDD[String] = sc.textFile(stations_filename)
  val temperatures: RDD[String] = sc.textFile(temperature_filename)
  val foo = sparkLocateTemperatures(year, stations, temperatures)


  //  val foo = Extraction.locateTemperatures(year, stations_filename, temperature_filename)
  //  val foo = Extraction.locateTemperatures(year, stations_filename, temperature_filename)
  //  val bar = Extraction.locationYearlyAverageRecords(foo)
  val bar = sparkAverageRecords(foo)
  //  println("locate temp size: " + foo.size)
  println("location Year avg rec size: " + bar.count)

}
