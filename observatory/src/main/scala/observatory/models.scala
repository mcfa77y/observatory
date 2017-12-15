package observatory

import java.time.LocalDate

import scala.math.{cos, log, tan, toRadians, Pi}

/**
  * Introduced in Week 1. Represents a location on the globe.
  *
  * @param lat Degrees of latitude, -90 ≤ lat ≤ 90
  * @param lon Degrees of longitude, -180 ≤ lon ≤ 180
  */
case class Location(lat: Double, lon: Double){
  var qk: String = ""
  var tile: Tile = null
  var zoom: Int = 8
  def toTile(_zoom: Int): Tile = {
    if (tile == null || zoom != _zoom) {
      tile = Tile(
        ((lon + 180.0) / 360.0 * (1 << _zoom)).toInt,
        ((1 - log(tan(toRadians(lat)) + 1 / cos(toRadians(lat))) / Pi) / 2.0 * (1 << _zoom)).toInt,
        _zoom)
      zoom = _zoom
    }
    tile
  }

  def toQK(_zoom: Int): String = {
    if(qk == "" || zoom != _zoom){

      qk = toTile(_zoom).toQK()
      zoom = _zoom
    }
    qk

  }
  override def equals(other: Any): Boolean = other match {
    case that: Location =>
      lat == that.lat &&
        lon == that.lon
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(lat, lon)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

}

case class Station(id: String, wban: String, location: Location)
case class TemperatureRecord(station_id: String, station_wban: String, localDate: LocalDate, temperature_F: Temperature)

/**
  * Introduced in Week 3. Represents a tiled web map tile.
  * See https://en.wikipedia.org/wiki/Tiled_web_map
  * Based on http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
  * @param x X coordinate of the tile
  * @param y Y coordinate of the tile
  * @param zoom Zoom level, 0 ≤ zoom ≤ 19
  */
case class Tile(x: Int, y: Int, zoom: Int) {
  def toLocation(): Location = {
    val n = Math.pow(2d, zoom)
    val lon_deg = x / n * 360.0 - 180.0d
    val lat_deg = Math.atan(Math.sinh(Math.PI * (1 - 2 * y / n))).toDegrees
    Location(lat_deg, lon_deg)
  }
  def toQK(): String = {
    var quadKey = ""
    for(i <- zoom until 0 by -1){
      var digit = 0
      val mask = 1 << (i - 1)
      if ((x & mask) != 0) digit += 1
      if ((y & mask) != 0) {
        digit += 1
        digit += 1
      }
      quadKey += digit
    }
    quadKey
  }
}

/**
  * Introduced in Week 4. Represents a point on a grid composed of
  * circles of latitudes and lines of longitude.
  * @param lat Circle of latitude in degrees, -89 ≤ lat ≤ 90
  * @param lon Line of longitude in degrees, -180 ≤ lon ≤ 179
  */
case class GridLocation(lat: Int, lon: Int) {
  def toLocation(): Location = Location(lat, lon)
  override def equals(other: Any): Boolean = other match {
    case that: GridLocation =>
      lat == that.lat &&
        lon == that.lon
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(lat, lon)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

/**
  * Introduced in Week 5. Represents a point inside of a grid cell.
  * @param x X coordinate inside the cell, 0 ≤ x ≤ 1
  * @param y Y coordinate inside the cell, 0 ≤ y ≤ 1
  */
case class CellPoint(x: Double, y: Double)

/**
  * Introduced in Week 2. Represents an RGB color.
  * @param red Level of red, 0 ≤ red ≤ 255
  * @param green Level of green, 0 ≤ green ≤ 255
  * @param blue Level of blue, 0 ≤ blue ≤ 255
  */
case class Color(red: Int, green: Int, blue: Int)
