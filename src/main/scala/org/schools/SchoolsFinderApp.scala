package org.schools

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SchoolsFinderApp {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val configFilePath = "application.conf"
    val config = ConfigFactory.parseFile(new File(configFilePath)).getConfig("schoolsfinder")
    val master = config.getString("master")
    val appName = config.getString("appName")

    val spark = SparkSession.builder().master(master).appName(appName).getOrCreate()

    val schoolsDataConfig = config.getConfig("schoolsdata")

    val ethnicsDataConfig = config.getConfig("ethnicsdata")

    val schoolsDF = SchoolDataService.getSchoolsData(spark, schoolsDataConfig, "Girls")

    val ethnicsDartfordData = EthnicDataService.getEthnicsDataByConstituency(spark, ethnicsDataConfig, "dartford")

    schoolsDF.cache()

    val girls_sutton_DF = schoolsDF.filter(col("Local authority") === "Sutton")

    val girls_bexely_DF = schoolsDF.filter(col("Local authority") === "Bexley")

    val girls_reading_DF = schoolsDF.filter(col("Local authority") === "Reading")

    val girls_watford_DF = schoolsDF.filter(col("Local authority") === "Hertfordshire").
      filter(col("constituency") === "Watford")

    val girls_buckinghamshire_DF = schoolsDF.filter(col("Local authority") === "Buckinghamshire").
      filter(col("constituency") === "Chesham and Amersham")

    val girls_dartford_DF = schoolsDF.filter(col("Local authority") === "Kent").
      filter(col("constituency") === "Dartford")

    println("sutton")
    girls_sutton_DF.show()

    println("bexely")
    girls_bexely_DF.show()

    println("reading")
    girls_reading_DF.show()

    println("Chesham and Amersham")
    girls_buckinghamshire_DF.show()

    println("watford")
    girls_watford_DF.show()

    println("dartford")
    girls_dartford_DF.show()

    girls_dartford_DF.join(ethnicsDartfordData,
      girls_dartford_DF.col("constituency") === ethnicsDartfordData.col("ConstituencyName")).show()
  }

}
