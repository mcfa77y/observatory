package observatory

import com.sksamuel.scrimage.{Image, RGBColor}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  def isAntipodal(j: Location, k: Location): Boolean = {
    j.lat == -k.lat && (j.lon == k.lon + 180 || j.lon == k.lon - 180)
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
//      c = cheap_acos(cheap_sin(theta1) * cheap_sin(theta2) + cheap_cos(theta1) * cheap_cos(theta2) * cheap_cos(Math.abs(lambda1 - lambda2)))

    }
    EARTH_RADIUS_KM * c

  }

  val TO_RADIANS = 0.01745329251
  val COS_ANS = Array(1, 0.984807753, 0.9396926209, 0.8660254039, 0.7660444434, 0.6427876101, 0.5000000005, 0.342020144, 0.1736481785, 0.0000000008948966625, -0.1736481767, -0.3420201423, -0.499999999, -0.6427876087, -0.7660444422, -0.866025403, -0.9396926202, -0.9848077527, -1, -0.9848077533, -0.9396926215, -0.8660254048, -0.7660444445, -0.6427876114, -0.5000000021, -0.3420201457, -0.1736481802, -0.000000002684689987, 0.1736481749, 0.3420201406, 0.4999999974, 0.6427876073, 0.7660444411, 0.8660254021, 0.9396926196, 0.9848077524)
  val ACOS_ANS = Array(0, 0.08726646255, 0.1745329251, 0.2617993877, 0.3490658502, 0.4363323128, 0.5235987753, 0.6108652379, 0.6981317004, 0.785398163, 0.8726646255, 0.9599310881, 1.047197551, 1.134464013, 1.221730476, 1.308996938, 1.396263401, 1.483529863, 1.570796326, 1.658062788, 1.745329251, 1.832595714, 1.919862176, 2.007128639, 2.094395101, 2.181661564, 2.268928026, 2.356194489, 2.443460951, 2.530727414, 2.617993877, 2.705260339, 2.792526802, 2.879793264, 2.967059727, 3.054326189, 3.141592654)

  def cheap_acos(x: Double): Double = {
    var index = (18.5 * x + 18.5).floor.toInt
    if(index < 0){
//      println("acos: offending x = " + x)
      index = 0
    } else if (index >= ACOS_ANS.size){
      index = ACOS_ANS.size-1
    }

    ACOS_ANS(index)
  }

  def cheap_cos(theta_degree: Double): Double = {
    val mod_theta_degree = theta_degree % 360
    val x = if((mod_theta_degree) > 0) mod_theta_degree else (mod_theta_degree) + 360
    var index: Int = ((x) * 0.1).floor.toInt
    if(index < 0){
      //      println("acos: offending x = " + x)
      index = 0
    } else if (index >= COS_ANS.size){
      index = COS_ANS.size-1
    }
    COS_ANS(index)
  }

  def cheap_sin(theta_degree: Double): Double = {
    cheap_cos(90 - theta_degree)
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    def createKey(l0: Location): (Location, Location) = {
      if (l0.lat * l0.lat + l0.lon * l0.lon < location.lat * location.lat + location.lon * location.lon)
        (l0, location) else (location, l0)
    }

    val distance_threshold_km = 2000
    val location_sample_count = 10
    val closest_locations = temperatures
      .filter(loc_temp => dist_KM(loc_temp._1, location) < distance_threshold_km)
      .take(location_sample_count)
//    if(closest_locations.size<location_sample_count){
//      println("closest_locations < "+location_sample_count+": " + closest_locations.size)
//    }
    val numerator = closest_locations.foldRight(0d)((data, acc) => acc + data._2 / dist_KM(data._1, location))
    val denominator = closest_locations.foldRight(0d)((data, acc) => acc + 1 / dist_KM(data._1, location))
    numerator / denominator
//    predictTemperatureSpark(sc.parallelize(temperatures.toList), location)
  }

//  def predictTemperatureSpark(temperatures: RDD[(Location, Temperature)], location: Location): Temperature = {
//    def createKey(l0: Location): (Location, Location) = {
//      if (l0.lat * l0.lat + l0.lon * l0.lon < location.lat * location.lat + location.lon * location.lon)
//        (l0, location) else (location, l0)
//    }
//
//    val distance_threshold_km = 100
//    val location_sample_count = 4
//    val closest_locations = temperatures
//      .filter(loc_temp => dist_KM(loc_temp._1, location) < distance_threshold_km)
//      .take(location_sample_count)
//
//    val numerator = closest_locations.foldRight(0d)((data, acc) => acc + data._2 / dist_KM(data._1, location))
//    val denominator = closest_locations.foldRight(0d)((data, acc) => acc + 1 / dist_KM(data._1, location))
//    numerator / denominator
//  }

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


    var maxTemp = 10000d
    var minTemp = -10000d
    var maxColor = Color(0, 0, 0)
    var minColor = Color(0, 0, 0)
    points.foreach(tc => {
      val temp = tc._1
      val color = tc._2
      if (temp > value && temp < maxTemp) {
        maxTemp = temp
        maxColor = color
      }
      if (temp < value && temp > minTemp) {
        minTemp = temp
        minColor = color
      }
    })
    val m: Double = (value - minTemp) / (maxTemp - minTemp)
    val red: Int = (m * (maxColor.red - minColor.red) + minColor.red).toInt
    val blue: Int = (m * (maxColor.blue - minColor.blue) + minColor.blue).toInt
    val green: Int = (m * (maxColor.green - minColor.green) + minColor.green).toInt
    Color(red, green, blue)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val image: Image = Image(360, 180)
    temperatures.foreach((loc_temp)=>{
      val loc = loc_temp._1
      val temperature = predictTemperature(temperatures, loc)
      val color = interpolateColor(colors, temperature)
      val x = loc.lon.toInt + 180
      val y = loc.lat.toInt + 90
      image.setPixel(x, y, RGBColor(color.red, color.green, color.blue).toPixel)
    })
    image.flipY


  }

//  def visualizeSpark(temperatures: RDD[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
//
//    val image: Image = Image(360, 180)
//    for (longitude <- -180 to 180 - 1; latitude <- -90 to 90 - 1) {
//      val temperature = predictTemperatureSpark(temperatures, Location(latitude, longitude))
//      val color = interpolateColor(colors, temperature)
//      val x = longitude + 180
//      val y = latitude + 90
//      image.setPixel(x, y, RGBColor(color.red, color.green, color.blue).toPixel)
//    }
//    image
//
//
//  }

}

