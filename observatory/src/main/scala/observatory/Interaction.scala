package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val n = Math.pow(2d, tile.zoom)
    val lon_deg = tile.x / n * 360.0 - 180.0d
    val lat_deg = Math.atan(Math.sinh(Math.PI * (1 - 2 * tile.y / n))).toDegrees
    Location(lat_deg, lon_deg)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param tile         Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val location = tileLocation(tile)

    val image = Image(256, 256)
    for (lat_deg <- location.lat.round.toInt to location.lat.round.toInt + 256;
         lon_deg <- location.lon.round.toInt to location.lon.round.toInt + 256) {
      val temperature = Visualization.predictTemperature(temperatures, location)
      val color = Visualization.interpolateColor(colors, temperature)
      image.setPixel(lon_deg, lat_deg, Pixel(color.red, color.green, color.blue, 127))
    }
    image
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
                           yearlyData: Iterable[(Year, Data)],
                           generateImage: (Year, Tile, Data) => Unit
                         ): Unit = {
    ???
  }

}
