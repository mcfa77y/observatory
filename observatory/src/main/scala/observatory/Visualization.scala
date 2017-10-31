package observatory

import com.sksamuel.scrimage.{Image, Pixel, RGBColor}

import scala.collection.mutable

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  def isAntipodal(j: Location, k: Location): Boolean = {
    j.lat == -k.lat && (j.lon == k.lon + 180 || j.lon == k.lon - 180)
  }

  val EARTH_RADIUS_KM = 6371d

  def dist(j: Location, k: Location): Double = {

    var c = 0d
    if (j.eq(k)) {
      c = 0d
    }
    else if (isAntipodal(j, k)) {
      c = Math.PI
    } else {
      val theta1 = j.lat.toRadians
      val lambda1 = j.lon.toRadians

      val theta2 = k.lat.toRadians
      val lambda2 = k.lon.toRadians

      c = Math.acos(Math.sin(theta1) * Math.sin(theta2) + Math.cos(theta1) * Math.cos(theta2) * Math.cos(Math.abs(lambda1 - lambda2)))
    }
    EARTH_RADIUS_KM * c

  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val distance_cache: mutable.Map[(Location, Location), Double] = mutable.Map()
    temperatures.map(_._1).foreach(loc => {
      val key: (Location, Location) = if (loc.lat * loc.lat + loc.lon * loc.lon < location.lat * location.lat + location.lon * location.lon) (loc, location) else (location, loc)
      distance_cache.getOrElseUpdate(key, dist(loc, location))
    })
    val numerator = temperatures.foldRight(0d)((data, acc) => acc + data._2 / distance_cache((data._1, location)))
    val denominator = temperatures.foldRight(0d)((data, acc) => acc + 1 / distance_cache((data._1, location)))
    numerator / denominator
  }

  /** @param points Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val maxTC:(Temperature, Color) = points.maxBy(_._1)
    val minTC:(Temperature, Color) = points.minBy(_._1)
    if(value >= maxTC._1){
      return maxTC._2
    }
    if(value <= minTC._1){
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
    Color(red, blue, green)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {

    val image: Image = Image(360, 180)
    for (longitude <- -180 to 180; latitude <- -90 to 90) {
      val temperature = predictTemperature(temperatures, Location(latitude, longitude))
      val color = interpolateColor(colors, temperature)
      val x = longitude + 180
      val y = latitude + 90
      image.setPixel(x, y, RGBColor(color.red, color.green, color.blue).toPixel)
    }
    image


  }

}

