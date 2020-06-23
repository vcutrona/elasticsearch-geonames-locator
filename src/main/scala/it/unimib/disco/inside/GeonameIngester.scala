package it.unimib.disco.inside

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

import scala.reflect.ClassTag

case class GeoPoint(lat: Double, lon: Double)

case class Geoname(geonameid: Int,
                   name: String,
                   asciiname: String,
                   alternatenames: List[String],
                   latitude: Float,
                   longitude: Float,
                   location: GeoPoint,
                   fclass: String,
                   fcode: String,
                   country: String,
                   cc2: List[String],
                   admin1: String,
                   admin2: String,
                   admin3: String,
                   admin4: String,
                   population: Long,
                   elevation: Long,
                   gtopo30: Long,
                   timezone: String,
                   moddate: String)

object GeonameIngester {

  def main(args: Array[String]) {
    val properties: Properties = new Properties()
    properties.load(new FileInputStream("application.properties"))

    val sparkMaster = properties.getProperty("spark_master")
    val esNode = properties.getProperty("elastic_host")
    val esPort = properties.getProperty("elastic_port")
    val esWanOnly = properties.getProperty("elastic_wan_only")

    val sparkSession = SparkSession.builder
      .master(sparkMaster)
      .config("es.nodes", esNode)
      .config("es.port", esPort)
      .config("es.nodes.wan.only", esWanOnly)
//      .config("es.nodes.discovery", value = false)
      .appName("GeonameIngester")
      .getOrCreate()

    implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] = org.apache.spark.sql.Encoders.kryo[A](ct)

    val geonameSchema = StructType(Array(
      StructField("geonameid", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("asciiname", StringType),
      StructField("alternatenames", StringType),
      StructField("latitude", FloatType),
      StructField("longitude", FloatType),
      StructField("fclass", StringType),
      StructField("fcode", StringType),
      StructField("country", StringType),
      StructField("cc2", StringType),
      StructField("admin1", StringType),
      StructField("admin2", StringType),
      StructField("admin3", StringType),
      StructField("admin4", StringType),
      StructField("population", LongType, nullable = false), // Asia population overflows Integer
      StructField("elevation", LongType, nullable = false),
      StructField("gtopo30", LongType, nullable = false),
      StructField("timezone", StringType),
      StructField("moddate", DateType)))

    val GEONAME_PATH = "downloads/allCountries.txt"

    val geonames = sparkSession.sqlContext.read
      .option("header", value = false)
      .option("quote", "")
      .option("delimiter", "\t")
      .option("maxColumns", 19)
      .schema(geonameSchema)
      .csv(GEONAME_PATH)
      .cache()

    val records = geonames.map {
      row =>
        val geoId = row.getInt(0)
        val name = row.getString(1)
        val asciiName = row.getString(2)
        val alterName = Option(row.getString(3)).map(_.split(",").map(_.trim).filterNot(_.isEmpty).toList).orNull
        val lat = row.getFloat(4)
        val lon = row.getFloat(5)
        val geoPoint = GeoPoint(lat, lon)
        val fClass = row.getString(6)
        val fCode = row.getString(7)
        val country = row.getString(8)
        val cc = Option(row.getString(9)).map(_.split(",").map(_.trim).filterNot(_.isEmpty).toList).orNull
        val admin1 = Option(row.getString(10)).orNull
        val admin2 = Option(row.getString(11)).orNull
        val admin3 = Option(row.getString(12)).orNull
        val admin4 = Option(row.getString(13)).orNull
        val population = if (row.get(14) == null) 0L else row.getLong(14)
        val elevation = if (row.get(15) == null) 0L else row.getLong(15)
        val gTopo = row.getLong(16)
        val timezone = row.getString(17)
        val modDate = row.getDate(18).toString

        Geoname(geoId, name, asciiName, alterName, lat, lon, geoPoint, fClass, fCode, country, cc, admin1,
          admin2, admin3, admin4, population, elevation, gTopo, timezone, modDate
        )
    }

    EsSpark.saveToEs(records.toJavaRDD, "geonames/geoname", Map("es.mapping.id" -> "geonameid"))

  }

}
