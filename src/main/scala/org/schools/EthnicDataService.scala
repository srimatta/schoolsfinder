package org.schools

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}

object EthnicDataService {

  def getEthnicsDataByConstituency(spark: SparkSession, ethnicsDataConfig: Config, constituencyName: String) = {

    val ethnicsDataPath = ethnicsDataConfig.getString("constituency_ethnics_data")
    val constituencyEthnicsDF = spark.read.option("inferSchema", "true").option("header", true).csv(ethnicsDataPath)

    constituencyEthnicsDF.select("ConstituencyName", "PopWhiteConst%",
      "PopAsianConst%", "PopBlackConst%").filter(lower(col("ConstituencyName")) === constituencyName)
  }

}
