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
    val constituencyEthnicsDataPath = schoolsDataConfig.getString("constituency_ethnics_data")


    val constituencyEthnicsDF = spark.read.option("inferSchema", "true").option("header", true).csv(constituencyEthnicsDataPath)


    val girlsDF = getSchoolsData(spark, schoolsDataConfig, "Girls")

    girlsDF.cache()

    val girls_sutton_DF = girlsDF.filter(col("Local authority") === "Sutton")

    val girls_bexely_DF = girlsDF.filter(col("Local authority") === "Bexley")

    val girls_reading_DF = girlsDF.filter(col("Local authority") === "Reading")

    val girls_watford_DF = girlsDF.filter(col("Local authority") === "Hertfordshire").
      filter(col("constituency") === "Watford")

    val girls_buckinghamshire_DF = girlsDF.filter(col("Local authority") === "Buckinghamshire").
      filter(col("constituency") === "Chesham and Amersham")

    val girls_dartford_DF = girlsDF.filter(col("Local authority") === "Kent").
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


    val dartford_constituencyEthnicsDF = constituencyEthnicsDF.select("ConstituencyName", "PopWhiteConst%",
      "PopAsianConst%", "PopBlackConst%").filter(lower(col("ConstituencyName")) === "dartford")

    dartford_constituencyEthnicsDF.show()

    /*
    girls_dartford_DF.join(dartford_constituencyEthnicsDF ,
      girls_dartford_DF.col("constituency") === dartford_constituencyEthnicsDF.col("ConstituencyName")).show()
    */
  }

  //gender either Boys, Girls or Mixed
  def getSchoolsData(spark : SparkSession, schoolsDataConfig : Config, gender :String = "Girls") = {

    val schoolsDataPath = schoolsDataConfig.getString("school_information")
    val schoolInspectionDataPath = schoolsDataConfig.getString("school_inspection")


    val schoolInspectionDF = spark.read.option("inferSchema", "true").option("header", true).csv(schoolInspectionDataPath)
    val schoolInformationDF = spark.read.option("inferSchema", "true").option("header", true).csv(schoolsDataPath)


    val schoolsDF = schoolInspectionDF.
      select("URN", "School name", "Ofsted phase", "Designated religious character",
        "Local authority", "Total number of pupils", "Postcode",
        "Overall effectiveness", "Total number of pupils", "Parliamentary constituency").
      join(schoolInformationDF.select("URN", "GENDER"), "URN").
      filter(col("Ofsted phase") === "Secondary").
      filter(col("GENDER") === gender).
      withColumnRenamed("Designated religious character", "religious").
      withColumnRenamed("Total number of pupils", "Strength").
      withColumnRenamed("Parliamentary constituency", "constituency")

    schoolsDF

  }
}
