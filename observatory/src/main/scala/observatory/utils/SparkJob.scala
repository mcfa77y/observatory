package observatory.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by joe on 11/2/17.
  */
trait SparkJob {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

//  implicit val spark:SparkSession = SparkSession
//    .builder()
//    .master("local[8]")
//    .appName("observatory")
//    .getOrCreate()

  val conf = new SparkConf().setAppName("observatory").setMaster("local[*]")
  implicit val sc = new SparkContext(conf)


}
