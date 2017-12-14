package observatory.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
/**
  * Created by joe on 11/2/17.
  */
trait SparkJob {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  def memoize[I, O](f: I => O): I => O = new mutable.HashMap[I, O]() {self =>
    override def apply(key: I) = {
      //      if(get(key).isDefined){
      //        println("memoize found: " + get(key))
      //      }
      self.synchronized(getOrElseUpdate(key, f(key)))
    }
  }
  implicit val spark:SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("observatory")
    .getOrCreate()

  implicit val sc: SparkContext = spark.sparkContext
//
//  val conf = new SparkConf().setAppName("observatory").setMaster("local[*]")
//  implicit val sc = new SparkContext(conf)


}
