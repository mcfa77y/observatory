package observatory

import java.io.File
import java.time.{LocalDate, ZonedDateTime}

import com.thesamet.spatial.KDTreeMap
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers



trait InteractionTest extends FunSuite with Checkers {

  test("qk") {
    assert(Tile(3,5,3).toQK() == "213")
  }
  test("make proper interaction image") {
    val color_scale: Iterable[(Temperature, Color)] = List((60, Color(255, 255, 255)),
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

    val t_loc_temp: RDD[(LocalDate, Location, Temperature)] = Extraction.locateTemperaturesSpark(year, stations_filename, temperature_filename)
    val temps = t_loc_temp.map((time_loc_temp) => (time_loc_temp._2, time_loc_temp._3))

    val image = Interaction.tile(temps.collect().toList, color_scale, tile)

    val zonedDateTimeIst = ZonedDateTime.now()
    val name = "spagetti_"+zonedDateTimeIst.getHour.toString +"_"+zonedDateTimeIst.getMinute.toString + ".png"
    val path = new File(".").getCanonicalFile +"/"+ name
    image.output(new File(path))

  }

  test("make interaction image") {
    val tile = Tile(0,0,0)

    val temps = List((Location(45.0,-90.0),20.0),
      (Location(45.0,90.0),0.0),
      (Location(0.0,0.0),10.0),
      (Location(-45.0,-90.0),0.0),
      (Location(-45.0,90.0),20.0))

    val colors = List((0.0,Color(255,0,0)),
      (10.0,Color(0,255,0)),
      (20.0,Color(0,0,255)))

    val image = Interaction.tile(temps, colors, tile)

    val zonedDateTimeIst = ZonedDateTime.now()
    val name = "spagetti_"+zonedDateTimeIst.getHour.toString +"_"+zonedDateTimeIst.getMinute.toString + ".png"
    val path = new File(".").getCanonicalFile +"/"+ name
    image.output(new File(path))

  }

  test("KD make interaction image") {
    val tile = Tile(0,0,0)

    val temps = List((Location(45.0,-90.0),20.0),
      (Location(45.0,90.0),0.0),
      (Location(0.0,0.0),10.0),
      (Location(-45.0,-90.0),0.0),
      (Location(-45.0,90.0),20.0))

    val temps_seq = temps.map( x => x._1 -> x._2)
//    val temps_seq = temps.map( x => (x._1.lon, x._1.lat) -> x._2)
    val temps_kd = KDTreeMap.fromSeq(temps_seq)
    val colors = List((0.0,Color(255,0,0)),
      (10.0,Color(0,255,0)),
      (20.0,Color(0,0,255)))

    val image = Interaction.tile_KD(temps_kd, colors, tile)

    val zonedDateTimeIst = ZonedDateTime.now()
    val name = "spagetti_"+zonedDateTimeIst.getHour.toString +"_"+zonedDateTimeIst.getMinute.toString + ".png"
    val path = new File(".").getCanonicalFile +"/"+ name
    image.output(new File(path))

  }


  test("KD make interaction proper image") {


    val color_scale: Iterable[(Temperature, Color)] = List((60, Color(255, 255, 255)),
      (32, Color(255, 0, 0)),
      (12, Color(255, 255, 0)),
      (0, Color(0, 255, 255)),
      (-15, Color(0, 0, 255)),
      (-27, Color(255, 0, 255)),
      (-50, Color(33, 0, 107)),
      (-60, Color(0, 0, 0)))
    val base_dir = "/"
    val year = 1975


    val tile = Tile(0,0,1)
    val temperature_filename = base_dir + year + ".csv"
    val stations_filename = base_dir + "stations.csv"

    val t_loc_temp: RDD[(LocalDate, Location, Temperature)] = Extraction.locateTemperaturesSpark(year, stations_filename, temperature_filename)
    val temps = t_loc_temp.map((time_loc_temp) => (time_loc_temp._2, time_loc_temp._3))


    val temps_seq = temps.map( x => (x._1.lon, x._1.lat) -> x._2).collect().toSeq
    val temps_kd = KDTreeMap.fromSeq(temps_seq)
    val colors = List((0.0,Color(255,0,0)),
      (10.0,Color(0,255,0)),
      (20.0,Color(0,0,255)))

    val image = Interaction.tile_KD(temps_kd, colors, tile)

    val zonedDateTimeIst = ZonedDateTime.now()
    val name = "spagetti_"+zonedDateTimeIst.getHour.toString +"_"+zonedDateTimeIst.getMinute.toString + ".png"
    val path = new File(".").getCanonicalFile +"/"+ name
    image.output(new File(path))

  }

}
