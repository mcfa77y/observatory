package observatory

import com.sksamuel.scrimage.{Image, Pixel, RGBColor}
import observatory.utils.SparkJob
import org.apache.spark.rdd.RDD

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
      c = Math.PI
    } else {
      val theta1 = j.lat
      val lambda1 = j.lon

      val theta2 = k.lat
      val lambda2 = k.lon

      c = Math.acos(Math.sin(theta1.toRadians) * Math.sin(theta2.toRadians) + Math.cos(theta1.toRadians) * Math.cos(theta2.toRadians) * Math.cos(Math.abs(lambda1.toRadians - lambda2.toRadians)))
      //c = cheap_acos(cheap_sin(theta1) * cheap_sin(theta2) + cheap_cos(theta1) * cheap_cos(theta2) * cheap_cos(Math.abs(lambda1 - lambda2)))

    }
    EARTH_RADIUS_KM * c

  }


  //  val TO_RADIANS = 0.01745329251
  //  val COS_ANS = Array(1, 0.984807753, 0.9396926209, 0.8660254039, 0.7660444434, 0.6427876101, 0.5000000005, 0.342020144, 0.1736481785, 0.0000000008948966625, -0.1736481767, -0.3420201423, -0.499999999, -0.6427876087, -0.7660444422, -0.866025403, -0.9396926202, -0.9848077527, -1, -0.9848077533, -0.9396926215, -0.8660254048, -0.7660444445, -0.6427876114, -0.5000000021, -0.3420201457, -0.1736481802, -0.000000002684689987, 0.1736481749, 0.3420201406, 0.4999999974, 0.6427876073, 0.7660444411, 0.8660254021, 0.9396926196, 0.9848077524)
  //  val ACOS_ANS = Array(0, 0.08726646255, 0.1745329251, 0.2617993877, 0.3490658502, 0.4363323128, 0.5235987753, 0.6108652379, 0.6981317004, 0.785398163, 0.8726646255, 0.9599310881, 1.047197551, 1.134464013, 1.221730476, 1.308996938, 1.396263401, 1.483529863, 1.570796326, 1.658062788, 1.745329251, 1.832595714, 1.919862176, 2.007128639, 2.094395101, 2.181661564, 2.268928026, 2.356194489, 2.443460951, 2.530727414, 2.617993877, 2.705260339, 2.792526802, 2.879793264, 2.967059727, 3.054326189, 3.141592654)
  val COS_ANS = for (deg <- 0 to 360) yield Math.cos(deg.toRadians)
  val ACOS_ANS = for (x <- -100 to 100) yield Math.acos(x / 100d)
  val acos_index = map_to(1, -1, 0, ACOS_ANS.size)

  def cheap_acos(x: Double): Double = {
    var index = acos_index(x).toInt
    if (index < 0) {
      //      println("acos: offending x = " + x)
      0
    }
    else if (index >= ACOS_ANS.size) {
      ACOS_ANS.size - 1
    }
    else {
      ACOS_ANS(index)
    }

  }

  val cos_index = map_to(0, 180, 0, COS_ANS.size)

  def cheap_cos(theta_degree: Double): Double = {
    if (theta_degree < 0) {
      cheap_cos(-theta_degree)
    }
    else if (theta_degree > 180) {
      -1 * COS_ANS(cos_index(theta_degree % 180).toInt)
    }
    else {
      COS_ANS(cos_index(theta_degree % 180).toInt)
    }


  }

  def cheap_sin(theta_degree: Double): Double = {
    cheap_cos(90 - theta_degree)
  }


  def get_closest_locations_old(temperatures: Iterable[(Location, Temperature)],
                            location: Location,
                            distance_threshold_km: Double,
                            location_sample_count: Int
                           ): List[(Location, Temperature)] = {
    //    def createKey(l0: Location): (Location, Location) = {
    //      if (l0.lat * l0.lat + l0.lon * l0.lon < location.lat * location.lat + location.lon * location.lon)
    //        (l0, location) else (location, l0)
    //    }
    //    var cache=collection.mutable.Map[(Location, Location), Double]()
    //    cache ++ distance_cache
    val closest_locations: List[(Location, Temperature)] = temperatures
      .filter(loc_temp => {
//          loc_temp._1.toQK(11).contains(location.toQK(distance_threshold_km))
        dist_KM(loc_temp._1, location) < distance_threshold_km
      })
      .take(location_sample_count).toList

    if (closest_locations.size < location_sample_count) {
      //      println("expanding radius to " + distance_threshold_km * 2)
      get_closest_locations_old(temperatures, location, distance_threshold_km * 2, location_sample_count)
    } else {
      closest_locations
    }
  }


  def get_closest_locations(temperatures: Iterable[(Location, Temperature)],
                            location: String,
                            distance_threshold_km: Int,
                            location_sample_count: Int
                           ): List[(Location, Temperature)] = {
    //    def createKey(l0: Location): (Location, Location) = {
    //      if (l0.lat * l0.lat + l0.lon * l0.lon < location.lat * location.lat + location.lon * location.lon)
    //        (l0, location) else (location, l0)
    //    }
    //    var cache=collection.mutable.Map[(Location, Location), Double]()
    //    cache ++ distance_cache
    val l = location.substring(0, distance_threshold_km)
    val closest_locations: List[(Location, Temperature)] = temperatures
      .filter(loc_temp => {
        val m = loc_temp._1.toQK(11)
        m.indexOf(l) == 0
        //        dist_KM(loc_temp._1, location) < distance_threshold_km
      })
      .take(location_sample_count).toList

    if (closest_locations.size < location_sample_count) {
      //      println("expanding radius to " + distance_threshold_km * 2)
      get_closest_locations(temperatures, location, distance_threshold_km - 1, location_sample_count)
    } else {
      closest_locations
    }
  }


  def get_closest_locations_spark(temperatures: RDD[(Location, Temperature)],
                                  location: Location,
                                  distance_threshold_km: Double,
                                  location_sample_count: Int
                                 ): List[(Location, Temperature)] = {
    //    def createKey(l0: Location): (Location, Location) = {
    //      if (l0.lat * l0.lat + l0.lon * l0.lon < location.lat * location.lat + location.lon * location.lon)
    //        (l0, location) else (location, l0)
    //    }
    //    var cache=collection.mutable.Map[(Location, Location), Double]()
    //    cache ++ distance_cache
    val closest_locations: List[(Location, Temperature)] = temperatures
      .filter(loc_temp => {
        dist_KM(loc_temp._1, location) < distance_threshold_km
      })
      .take(location_sample_count).toList

    if (closest_locations.size < location_sample_count) {
      //      println("expanding radius to " + distance_threshold_km * 2)
      get_closest_locations_spark(temperatures, location, distance_threshold_km * 2, location_sample_count)
    } else {
      closest_locations
    }
  }

//  def get_closest_locations_qk(temperatures: RDD[(String, Temperature)],
//                                  location: Location,
//                                  distance_threshold_km: Double,
//                                  location_sample_count: Int
//                                 ): List[(Location, Temperature)] = {
//
//    val closest_locations: List[(Location, Temperature)] = temperatures
//      .filter(loc_temp => {
//        loc_temp._1
//      })
//      .take(location_sample_count).toList
//
//    if (closest_locations.size < location_sample_count) {
//      //      println("expanding radius to " + distance_threshold_km * 2)
//      get_closest_locations_spark(temperatures, location, distance_threshold_km * 2, location_sample_count)
//    } else {
//      closest_locations
//    }
//  }


  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {


    val distance_threshold_km = 8
    val location_sample_count = Math.min(10, temperatures.size)


    val closest_locations: List[(Location, Temperature)] = get_closest_locations(temperatures, location.toQK(distance_threshold_km), distance_threshold_km, location_sample_count)

    val foo = closest_locations.find(_._1 == location)
    if (foo.isDefined) {
      return foo.get._2
    }
    val zero = closest_locations.find((loc_temp) => dist_KM(loc_temp._1, location) == 0)
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
    //    predictTemperatureSpark(sc.parallelize(temperatures.toList), location)
  }

  def predictTemperatureSpark(temperatures: RDD[(Location, Temperature)], location: Location): Temperature = {

    val distance_threshold_km = 100
    val location_sample_count = 4
    val closest_locations = get_closest_locations_spark(temperatures, location, distance_threshold_km, location_sample_count)

    val numerator = closest_locations.foldRight(0d)((data, acc) => acc + data._2 / dist_KM(data._1, location))
    val denominator = closest_locations.foldRight(0d)((data, acc) => acc + 1 / dist_KM(data._1, location))
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
    image.flipY


  }

}

