package observatory

import java.io.File
import java.time.{LocalDate, ZonedDateTime}

import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait Visualization2Test extends FunSuite with Checkers {


  test("make visualization grid image") {
    val tile = Tile(0,0,0)

    val temps = List((Location(45.0,-90.0),20.0),
      (Location(45.0,90.0),0.0),
      (Location(0.0,0.0),10.0),
      (Location(-45.0,-90.0),0.0),
      (Location(-45.0,90.0),20.0))

    val colors = List((0.0,Color(255,0,0)),
      (10.0,Color(0,255,0)),
      (20.0,Color(0,0,255)))

    val glFN = Manipulation.makeGrid(temps)
    val image = Visualization2.visualizeGrid(glFN, colors, tile)

    val zonedDateTimeIst = ZonedDateTime.now()
    val name = "v_grid_"+zonedDateTimeIst.getHour.toString +"_"+zonedDateTimeIst.getMinute.toString + ".png"
    val path = new File(".").getCanonicalFile +"/"+ name
    image.output(new File(path))

  }

  test("make proper v2 image") {
    val colors: Iterable[(Temperature, Color)] = List((60, Color(255, 255, 255)),
      (32, Color(255, 0, 0)),
      (12, Color(255, 255, 0)),
      (0, Color(0, 255, 255)),
      (-15, Color(0, 0, 255)),
      (-27, Color(255, 0, 255)),
      (-50, Color(33, 0, 107)),
      (-60, Color(0, 0, 0)))
    val base_dir = "/"
    val year = 1975


    val tile = Tile(0,0,0)
    val temperature_filename = base_dir + year + ".csv"
    val stations_filename = base_dir + "stations.csv"

    val t_loc_temp: RDD[(LocalDate, Location, Temperature)] =
      Extraction.locateTemperaturesSpark(year, stations_filename, temperature_filename)
        .sample(false, 0.01, 0L)
    val temps = t_loc_temp.map((time_loc_temp) => (time_loc_temp._2, time_loc_temp._3)).collect().toList

    val glFN = Manipulation.makeGrid(temps)
    val image = Visualization2.visualizeGrid(glFN, colors, tile)

    val zonedDateTimeIst = ZonedDateTime.now()
    val name = "v2_proper_image"+zonedDateTimeIst.getHour.toString +"_"+zonedDateTimeIst.getMinute.toString + ".png"
    val path = new File(".").getCanonicalFile +"/"+ name
    image.output(new File(path))

  }
}