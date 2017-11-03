package observatory


import java.io._

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers {
  val color_scale: Iterable[(Temperature, Color)] = List((60, Color(255, 255, 255)),
    (32, Color(255, 0, 0)),
    (12, Color(255, 255, 0)),
    (0, Color(0, 255, 255)),
    (-15, Color(0, 0, 255)),
    (-27, Color(255, 0, 255)),
    (-50, Color(33, 0, 107)),
    (-60, Color(0, 0, 0)))

  def generateLRange(size: Int, count: Int): Array[Int] =  {
    val degree_start = -size + size / (count + 1)
    val degree_end = size - size / (count + 1)
    (for (degree <- degree_start to degree_end by 2 * size / (count + 1)) yield degree)(collection.breakOut)
  }

  def generateStations(count: Int): Array[String] = {
    val lat_range = generateLRange(90, count)
    val lon_range = generateLRange(180, count)
    for (longitude <- lon_range; latitude <- lat_range) yield {
      val latlon = latitude + "_" + longitude
      Array("id_" + latlon, "wban_id_" + latlon, latitude, longitude).mkString(",")
    }
  }

//  val conf = new SparkConf().setAppName("observatory").setMaster("local[*]")
//  val sc = new SparkContext(conf)
  def generateTemps(count: Int): Array[String] = {
    val MONTH = "07"
    val DATE = "04"
    val TEMP_F = "73"

    val lat_range = generateLRange(90, count)
    val lon_range = generateLRange(180, count)

    val r = scala.util.Random
    val start = -100
    val end = 160
    for (longitude <- lon_range; latitude <- lat_range) yield {
      val latlon = latitude + "_" + longitude
      Array("id_" + latlon, "wban_id_" + latlon, MONTH, DATE, start + r.nextInt( (end - start) + 1 )  ).mkString(",")
    }
  }

  test("stations with no location are ignored x") {
//    val base_dir = "/src/main/resources/"
    val base_dir = "/"
    val year = 1975

    val STATION_COUNT = 10
    //    val stations: RDD[Station] = sc.parallelize(Array((Station("id_0", "wban_0", Location(100d, 100d)))))
//    val stations: RDD[String] = sc.parallelize(generateStations(STATION_COUNT))
//    val temperatures: RDD[String] = sc.parallelize(generateTemps(STATION_COUNT))

//    val foo = Extraction.sparkLocateTemperatures(year, stations, temperatures)

    val temperature_filename = base_dir + year + ".csv"
    val stations_filename = base_dir + "stations.csv"
    val foo = Extraction.locateTemperaturesSpark(year, stations_filename, temperature_filename)
//    val foo = Extraction.locateTemperaturesSpark(year, stations, temperatures)
    val bar = Extraction.averageRecordsSpark(foo)
//    val image = Visualization.visualize(bar.collect.toList, color_scale)
    val image = Visualization.visualizeSpark(bar, color_scale)
    //    assert(foo.size == 1)
    image.output(new File("/home/joe/Sites/scala_course/observatory/spaghetti.png"))

  }
}
