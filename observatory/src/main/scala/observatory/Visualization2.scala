package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00   Top-left value
    * @param d01   Bottom-left value
    * @param d10   Top-right value
    * @param d11   Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
                             point: CellPoint,
                             d00: Temperature,
                             d01: Temperature,
                             d10: Temperature,
                             d11: Temperature
                           ): Temperature = {
    val x = point.x
    val y = point.y
    val t = d00 * (1.0 - x) * (1.0 - y) +
            d10 * x * (1.0 - y) +
            d01 * (1.0 - x) * y +
            d11 * x * y
    t
  }

  /**
    * @param grid   Grid to visualize
    * @param colors Color scale to use
    * @param tile   Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
                     grid: GridLocation => Temperature,
                     colors: Iterable[(Temperature, Color)],
                     tile: Tile
                   ): Image = {
    val zoom_until_offset = 8

    val height = Math.pow(2, zoom_until_offset).toInt
    val width = height
    val image = Image(width, height)

    val t_p = Interaction.rec_createChildrenTilesZoomUntil(tile, tile.zoom + zoom_until_offset).par.map(sub_tile => {
      val lat = sub_tile.toLocation().lat
      val lon = sub_tile.toLocation().lon

      val d00 = grid(GridLocation(lat.floor.toInt, lon.floor.toInt))
      val d01 = grid(GridLocation(lat.floor.toInt, lon.ceil.toInt))
      val d10 = grid(GridLocation(lat.ceil.toInt, lon.floor.toInt))
      val d11 = grid(GridLocation(lat.ceil.toInt, lon.ceil.toInt))

      val x = Visualization.map_to(lon.floor, 0, lon.ceil, 1)(lon)
      val y = Visualization.map_to(lat.floor, 0, lat.ceil, 1)(lat)
      val cellPoint = CellPoint(x,y)

      val temp = bilinearInterpolation(cellPoint, d00, d01, d10, d11)

      val color = Visualization.interpolateColor(colors, temp)
      val pixel = Pixel(color.red, color.green, color.blue, 127)
      (sub_tile, pixel)
    })

    t_p.par.foreach(_ match {
      case (tile, pixel) => {
        {
          image.setPixel(tile.x % width, tile.y % height, pixel)
        }
      }
    })

    image.scaleTo(256,256)


  }

}
