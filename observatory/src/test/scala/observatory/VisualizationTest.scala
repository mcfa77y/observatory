package observatory


import observatory.Extraction.sc
import observatory.Main.{stations_filename, temperature_filename}
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers {
  val color_scale: Iterable[(Temperature, Color)] = List((60, Color(255, 255, 255)),
    (32, Color(255, 0,   0)),
    (12, Color(255, 255, 0)),
    (0, Color( 0,   255, 255)),
    (-15, Color(0   ,0   ,255)),
    (-27, Color(255 ,0   ,255)),
    (-50, Color(33  ,0   ,107)),
    (-60, Color(0   ,0   ,0)))

  test("stations with no location are ignored x"){
    val base_dir = "src/main/resources/"
    val year = 1975
    val temperature_filename = base_dir + year + "_test.csv"
    val stations_filename = base_dir + "stations_empty_test.csv"
//    val stations: RDD[Station] = sc.parallelize(Array((Station("id_0", "wban_0", Location(100d, 100d)))))
    val stations: RDD[String] = sc.parallelize(Array("010010, wban_0, 100, 100"))
//    val temperatures: RDD[String] = sc.textFile(temperature_filename)
    val temperatures: RDD[String] = sc.parallelize(Array("010010,wban_0,01,04,-40.0"))

    val foo = Extraction.sparkLocateTemperatures(year, stations, temperatures)
    val bar = Extraction.sparkAverageRecords(foo)
    Visualization.visualize(bar.collect.toSeq , color_scale)
//    assert(foo.size == 1)

  }
}
