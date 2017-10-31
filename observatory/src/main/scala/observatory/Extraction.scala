package observatory

import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
//import spark.implicites._
/**
  * 1st milestone: data extraction
  */
object Extraction {


  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val conf = new SparkConf().setAppName("observatory").setMaster("local[*]")
  val sc = new SparkContext(conf)
  //  val sc: SparkSession = SparkSession.builder().appName("observatory").master("local").getOrCreate()

  def fahrenheitToCelsius(f: Double): Double =
    (f - 32.0) * (5.0 / 9.0)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    val stations: RDD[String] = sc.textFile(this.getClass.getResource(stationsFile).getPath, 8)
    val temperatures: RDD[String] = sc.textFile(this.getClass.getResource(temperaturesFile).getPath, 8)
    sparkLocateTemperatures(year, stations, temperatures).collect.toSeq

  }

  def foobar(data: String, pred: String => Boolean): Boolean = {
    !data.isEmpty && pred(data)
  }

  def doubleNot0(data: String): Boolean = {
    data.toDouble != 0
  }

  def sparkLocateTemperatures(year: Year, stations: RDD[String], temperatures: RDD[String]): RDD[(LocalDate, Location, Temperature)] = {
    val s = stations
      .map(row => row.split(","))
      .filter(_.length > 3)
      .filter(data => foobar(data(3), doubleNot0) && foobar(data(3), doubleNot0))
      .map(data => Station(data(0), data(1), Location(data(2).toDouble, data(3).toDouble)))
      .keyBy(station => station.id.trim+"_"+ station.wban.trim)
    //    println("s: " + s.collect().length)
    val t = temperatures
      .map(_.split(","))
      .filter(_.length > 3)
      .filter(data => foobar(data(4), (temp: String) => temp.toDouble < 9999.9))
      .map(data => TemperatureRecord(data(0), data(1), LocalDate.of(year, data(2).toInt, data(3).toInt), data(4).toDouble))
      .keyBy(temp => temp.station_id.trim+"_"+temp.station_wban.trim)

    val ts = s.join(t)

    ts.values.map((stn_tmp) => {
      val stn = stn_tmp._1
      val tmp_rec = stn_tmp._2
      val temp_C: Temperature = fahrenheitToCelsius(tmp_rec.temperature_F)
      (tmp_rec.localDate, stn.location, temp_C)
    })

  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {

    sparkAverageRecords(sc.parallelize(records.toList)).collect.toSeq
  }

  // Added method:
  def sparkAverageRecords(records: RDD[(LocalDate, Location, Temperature)]): RDD[(Location, Temperature)] = {
    //    val a = records.groupBy(record => record._2)
    ////    val ac = a.collect
    //    val b = a.mapValues(x => x.foldRight((0d, 0))((row, acc ) => (row._3 + acc._1, acc._2 + 1)))
    ////    val bc = b.collect
    //
    //    val d = b.mapValues(y => 1.0 * y._1 / y._2)

    //    println("ac: " + ac)
    //    println("bc: " + bc)

    //    println("dc: " + d.collect)
    records
      .groupBy(record => record._2)
      .mapValues(x => x.foldRight((0d, 0))((row, acc ) => (row._3 + acc._1, acc._2 + 1)))
      .mapValues(y => 1.0 * y._1 / y._2)

  }

}
