package observatory

import com.sksamuel.scrimage.{Image, Pixel, RGBColor}
import observatory.utils.SparkJob

/**
  * 3rd milestone: interactive visualization
  */
object Interaction extends SparkJob{

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
  def make_pairs(start_x:Int, start_y:Int, end_x:Int, end_y:Int): List[(Int, Int)] ={
    val x_range = start_x to end_x
    val y_range = start_y to end_y
    for (x <- x_range.toList;
         y <- y_range.toList)
      yield (x,y)
  }
  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param tile         Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val height = 256
    val width = 256
    val image = Image(width, height)
    bar(temperatures, colors,tile).foreach((tile_pixel) => {
      val tile = tile_pixel._1
      val pixel = tile_pixel._2
      image.setPixel(tile.x, tile.y, pixel)
    })
    image
  }

  def bar(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile:Tile): List[(Tile, Pixel)] = {
    if(tile.zoom == 8){
      val location = tileLocation(tile)
      val temp = Visualization.predictTemperature(temperatures, location)
      val color = Visualization.interpolateColor(colors, temp)
      val pixel = RGBColor(color.red, color.green, color.blue, 127).toPixel
      List((tile, pixel))
    }
    else {
      val x = 2 * tile.x
      val y = 2 * tile.y
      val zoom = tile.zoom + 1
      val offsets = for (x_offset <- 0 to 1; y_offset <- 0 to 1) yield Tile(x + x_offset, y + y_offset, zoom)

      bar(temperatures, colors, offsets(0)) ++
        bar(temperatures, colors, offsets(1)) ++
        bar(temperatures, colors, offsets(2)) ++
        bar(temperatures, colors, offsets(3));
    }
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
