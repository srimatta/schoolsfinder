package org.schools

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SchoolDataService {

  //gender either Boys, Girls or Mixed
  def getSchoolsData(spark: SparkSession, schoolsDataConfig: Config, gender: String = "Girls") = {

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
