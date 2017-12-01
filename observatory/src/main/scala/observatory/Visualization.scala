package observatory

import com.sksamuel.scrimage.{Image, Pixel, RGBColor}
import observatory.utils.SparkJob
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.util.FastMath.{PI, acos, cos, sin, min, abs}

/**
  * 2nd milestone: basic visualization
  */
object Visualization extends SparkJob {
  def isAntipodal(j: Location, k: Location): Boolean = {
    j.lat == -k.lat && (j.lon == k.lon + 180 || j.lon == k.lon - 180)
  }

  class Memoize1[-T, +R](f: T => R) extends (T => R) {

    import scala.collection.mutable

    private[this] val vals = mutable.Map.empty[T, R]

    def apply(x: T): R = {
      if (vals.contains(x)) {
        vals(x)
      }
      else {
        val y = f(x)
        vals += ((x, y))
        y
      }
    }
  }

  object Memoize1 {
    def apply[T, R](f: T => R) = new Memoize1(f)
  }

  val EARTH_RADIUS_KM = 6371d

  def dist_KM(j: Location, k: Location): Double = {

    var c = 0d
    if (j.eq(k)) {
      c = 0d
    }
    else if (isAntipodal(j, k)) {
      c = PI
    } else {
      val theta1 = j.lat
      val lambda1 = j.lon

      val theta2 = k.lat
      val lambda2 = k.lon

      c = acos(sin(theta1.toRadians) * sin(theta2.toRadians) + cos(theta1.toRadians) * cos(theta2.toRadians) * cos(abs(lambda1.toRadians - lambda2.toRadians)))
    }
    EARTH_RADIUS_KM * c

  }


  def get_closest_locations(temperatures: Iterable[(Location, Temperature)],
                            location: Location,
                            distance_threshold_km: Double,
                            location_sample_count: Int
                           ): List[(Location, Temperature)] = {

    val closest_locations: List[(Location, Temperature)] = temperatures.par
      .filter(loc_temp => dist_KM(loc_temp._1, location) < distance_threshold_km)
      .take(location_sample_count).toList

    if (closest_locations.size < location_sample_count) {
      //      println("expanding radius to " + distance_threshold_km * 2)
      get_closest_locations(temperatures, location, distance_threshold_km * 2, location_sample_count)
    } else {
      closest_locations
    }
  }


  def get_closest_locations_qk(temperatures: Iterable[(Location, Temperature)],
                               location: String,
                               start_qk_len: Int,
                               location_sample_count: Int
                           ): List[(Location, Temperature)] = {
    val l = location.substring(0, start_qk_len)
    val closest_locations: List[(Location, Temperature)] = temperatures.par
      .filter(loc_temp => {
        val loc = loc_temp._1
        val m = loc.toQK(11)
        
        m.indexOf(l) == 0
      })
      .take(location_sample_count).toList

    if (closest_locations.size < location_sample_count) {
      get_closest_locations_qk(temperatures, location, start_qk_len - 1, location_sample_count)
    } else {
      closest_locations
    }
  }

  def get_closest_locations_qk_2(temperatures: Iterable[(Location, Temperature)],
                               location: String,
                               location_sample_count: Int
                              ): List[(Location, Temperature)] = {

    val closest_locations: List[(Location, Temperature)] = temperatures.par
      .filter(loc_temp => {
        val m = loc_temp._1.toQK(11)
        m.indexOf(location) == 0
      })
      .take(location_sample_count).toList

    if (closest_locations.size < location_sample_count) {
      get_closest_locations_qk_2(temperatures, location.substring(0, location.size-1), location_sample_count)
    } else {
      closest_locations
    }
  }



  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val location_sample_count = min(10, temperatures.size)

    val distance_threshold_km = 100.0
    val closest_locations: List[(Location, Temperature)] = get_closest_locations(temperatures, location, distance_threshold_km, location_sample_count)

//    val distance_threshold_qk = 8
//    val closest_locations: List[(Location, Temperature)] = get_closest_locations_qk(temperatures, location.toQK(distance_threshold_qk), distance_threshold_qk, location_sample_count)

    val foo = closest_locations.par.find(_._1 == location)
    if (foo.isDefined) {
      return foo.get._2
    }
    val zero = closest_locations.par.find((loc_temp) => dist_KM(loc_temp._1, location) == 0)
    if (zero.isDefined) {
      return zero.get._2
    }
    val numerator = closest_locations.foldRight(0d)((data, acc) => acc + data._2 / dist_KM(data._1, location))
    val denominator = closest_locations.foldRight(0d)((data, acc) => acc + 1 / dist_KM(data._1, location))
    if ((numerator / denominator).isNaN || denominator == 0) {
      println("things are not right there is a NaN here!")
      println("\tsize: " + closest_locations.size)
      println("\tresult tl: " + numerator + "/" + denominator)
    }

    numerator / denominator
  }


  /** @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val maxTC: (Temperature, Color) = points.maxBy(_._1)
    val minTC: (Temperature, Color) = points.minBy(_._1)
    if (value >= maxTC._1) {
      return maxTC._2
    }
    if (value <= minTC._1) {
      return minTC._2
    }
    //    points.foreach((loc_temp)=>{
    //      println("\ttemp: " + loc_temp._1 +" color: " + loc_temp._2)
    //    })
    var maxTemp = Double.MaxValue
    var minTemp = Double.MinValue
    var maxColor = Color(0, 0, 0)
    var minColor = Color(0, 0, 0)
    points.foreach(tc => {
      val temp = tc._1
      val color = tc._2
      if (value < temp && temp <= maxTemp) {
        maxTemp = temp
        maxColor = color
      }
      if (minTemp < temp && temp <= value) {
        minTemp = temp
        minColor = color
      }
    })
    val m: Double = (value - minTemp) / (maxTemp - minTemp)
    val red: Int = (m * (maxColor.red - minColor.red) + minColor.red).round.toInt
    val blue: Int = (m * (maxColor.blue - minColor.blue) + minColor.blue).round.toInt
    val green: Int = (m * (maxColor.green - minColor.green) + minColor.green).round.toInt
    if (red == 0 && blue == 0 && green == 0) {
      println("things are black")
    }
    //    println("\tresult c: " + Color(red, green, blue))
    Color(red, green, blue)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    visualizeSpark(sc.parallelize(temperatures.toList), colors)
//    val image: Image = Image(360, 180)
//
//    for (longitude <- -180 to 180 - 1; latitude <- -90 to 90 - 1) {
//      val temperature = predictTemperature(temperatures, Location(latitude, longitude))
//      if (temperature.isNaN) {
//        println("this is not the temp you're looking for NaN")
//      }
//      val color = interpolateColor(colors, temperature)
//      val x = longitude + 180
//      val y = latitude + 90
//      image.setPixel(x, y, RGBColor(color.red, color.green, color.blue).toPixel)
//    }
//
//
//    //    temperatures.foreach((loc_temp)=>{
//    //      val loc = loc_temp._1
//    //      val temperature = predictTemperature(temperatures, loc)
//    //      if(temperature.isNaN){
//    //        println("this is not the temp you're looking for NaN")
//    //      }
//    //      val color = interpolateColor(colors, temperature)
//    //      val x = loc.lon.toInt + 180
//    //      val y = loc.lat.toInt + 90
//    //      image.setPixel(x, y, RGBColor(color.red, color.green, color.blue).toPixel)
//    //    })
//    image.flipY


  }

  def map_to(domain_x: Double, range_x: Double, domain_y: Double, range_y: Double) = (domain_x_prime: Double) => {
    val m = (range_y - domain_y) / (range_x - domain_x)
    val b = range_y - m * range_x
    m * domain_x_prime + b
  }

  def visualizeSpark(temperatures: RDD[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    case class Pixel_Record(x: Int, y: Int, pixel: Pixel)
    val width = 360
    val height = 180
    val image: Image = Image(width, height)
    val lon_deg_range = (-180 to 180 - 1)
    val lat_deg_range = (-90 to 90 - 1)

    val get_x = map_to(-180, 180, 0, 360)
    val get_y = map_to(-90, 90, 0, 180)

    val t_list = temperatures.collect.toList

    def some_func(location: Location): Pixel_Record = {
      val temperature = predictTemperature(t_list, location)
      val color = interpolateColor(colors, temperature)
      val x = get_x(location.lon).toInt
      val y = get_y(location.lat).toInt
      Pixel_Record(x, y, RGBColor(color.red, color.green, color.blue).toPixel)
    }


    // make lat lon pairs
//    val world_location: List[Location] =
//      for (lat_deg <- lat_deg_range.toList;
//           lon_deg <- lon_deg_range.toList)
//        yield Location(lat_deg, lon_deg)
//    val pixels = sc.parallelize(world_location)
//      .map(some_func).collect.toSeq

        val pixels = temperatures.map(_._1)
          .map(some_func).collect.toSeq

    //      val pixels = sc.parallelize(0 to (width) * (height) -1).map(index => {
    //        some_func(Location(index/width, index%width))
    //      }).collect.toSeq

    pixels
      .filter(pixel_record => pixel_record.x < width && pixel_record.y < height)
      .foreach((pixel_record: Pixel_Record) => {
        image.setPixel(pixel_record.x, pixel_record.y, pixel_record.pixel)
      })

    //      for (longitude <- -180 to 180 - 1; latitude <- -90 to 90 - 1) {
    //        val temperature = predictTemperatureSpark(temperatures, Location(latitude, longitude))
    //        val color = interpolateColor(colors, temperature)
    //        val x = longitude + 180
    //        val y = latitude + 90
    //        image.setPixel(x, y, RGBColor(color.red, color.green, color.blue).toPixel)
    //      }
    image.flipY.scale(2)



  }

}

