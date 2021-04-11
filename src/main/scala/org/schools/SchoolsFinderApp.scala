package org.schools

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
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

    val schoolsDF = SchoolDataService.getSchoolsData(spark, schoolsDataConfig, "Girls", ofstedPhase = "Secondary")

    schoolsDF.cache()

    val byLocalAuthority = Window.partitionBy("Local authority")

    val ofstedRatingCondition = (colName : String) => when(col(colName) <=> "1", "Outstanding").
      when(col(colName) <=> "2", "Good").
      otherwise("Requires improvement")

    schoolsDF.filter(col("religious").isin("Does not apply", "None")).
      withColumn("oftest rating", ofstedRatingCondition("Overall effectiveness")).
      withColumn("GirlsSchoolsCountByLocalAuthority", count("GENDER").over(byLocalAuthority)).
      withColumn("TotalStrengthByLocalAuthority", sum(schoolsDF("Strength")).over(byLocalAuthority)).
      orderBy(desc("GirlsSchoolsCountByLocalAuthority"), col("Local authority"), col("constituency")).
      filter(col("GirlsSchoolsCountByLocalAuthority") > 1)
      .show(150)

    /**
     * Results
     * +------+--------------------+------------+--------------+--------------------+--------+--------+---------------------+--------------------+------+--------------------+---------------------------------+-----------------------------+
     * |   URN|         School name|Ofsted phase|     religious|     Local authority|Strength|Postcode|Overall effectiveness|        constituency|GENDER|       oftest rating|GirlsSchoolsCountByLocalAuthority|TotalStrengthByLocalAuthority|
     * +------+--------------------+------------+--------------+--------------------+--------+--------+---------------------+--------------------+------+--------------------+---------------------------------+-----------------------------+
     * |136379|Highworth Grammar...|   Secondary|Does not apply|                Kent|    1516|TN24 8UD|                    1|             Ashford| Girls|         Outstanding|                               16|                      18620.0|
     * |118840|Simon Langton Gir...|   Secondary|          None|                Kent|    1136| CT1 3EW|                    2|          Canterbury| Girls|                Good|                               16|                      18620.0|
     * |137250|Wilmington Gramma...|   Secondary|          None|                Kent|     918| DA2 7BB|                    1|            Dartford| Girls|         Outstanding|                               16|                      18620.0|
     * |144100|Dartford Grammar ...|   Secondary|          None|                Kent|    1197| DA1 2NT|                    1|            Dartford| Girls|         Outstanding|                               16|                      18620.0|
     * |118785|Dartford Science ...|   Secondary|Does not apply|                Kent|     786| DA1 2LY|                    2|            Dartford| Girls|                Good|                               16|                      18620.0|
     * |118806|Dover Grammar Sch...|   Secondary|Does not apply|                Kent|     911|CT16 2PZ|                    1|               Dover| Girls|         Outstanding|                               16|                      18620.0|
     * |137837|The Folkestone Sc...|   Secondary|          None|                Kent|    1180|CT20 3RB|                    1|Folkestone and Hythe| Girls|         Outstanding|                               16|                      18620.0|
     * |118788|Northfleet School...|   Secondary|Does not apply|                Kent|    1006|DA11 8AQ|                    2|           Gravesham| Girls|                Good|                               16|                      18620.0|
     * |137834|Mayfield Grammar ...|   Secondary|          None|                Kent|    1137|DA11 0JE|                    1|           Gravesham| Girls|         Outstanding|                               16|                      18620.0|
     * |136582|Invicta Grammar S...|   Secondary|          None|                Kent|    1544|ME14 5DS|                    1|Maidstone and The...| Girls|         Outstanding|                               16|                      18620.0|
     * |118836|Maidstone Grammar...|   Secondary|          None|                Kent|    1240|ME16 0SF|                    1|Maidstone and The...| Girls|         Outstanding|                               16|                      18620.0|
     * |136305|Highsted Grammar ...|   Secondary|          None|                Kent|     853|ME10 4PT|                    1|Sittingbourne and...| Girls|         Outstanding|                               16|                      18620.0|
     * |136417|Tonbridge Grammar...|   Secondary|          None|                Kent|    1179| TN9 2JR|                    1|Tonbridge and Mal...| Girls|         Outstanding|                               16|                      18620.0|
     * |136455|Weald of Kent Gra...|   Secondary|          None|                Kent|    1674| TN9 2JP|                    1|Tonbridge and Mal...| Girls|         Outstanding|                               16|                      18620.0|
     * |137104|Hillview School f...|   Secondary|          None|                Kent|    1325| TN9 2HE|                    2|Tonbridge and Mal...| Girls|                Good|                               16|                      18620.0|
     * |118789|Tunbridge Wells G...|   Secondary|          None|                Kent|    1018| TN4 9UJ|                    1|     Tunbridge Wells| Girls|         Outstanding|                               16|                      18620.0|
     * |137346|Hillcrest School ...|   Secondary|Does not apply|          Birmingham|     579| B32 3AE|                    2|Birmingham, Edgba...| Girls|                Good|                               10|                       9593.0|
     * |136592|Lordswood Girls' ...|   Secondary|          None|          Birmingham|     970| B17 8QB|                    1|Birmingham, Edgba...| Girls|         Outstanding|                               10|                       9593.0|
     * |103483|Hodge Hill Girls'...|   Secondary|Does not apply|          Birmingham|     750| B36 8EY|                    2|Birmingham, Hodge...| Girls|                Good|                               10|                       9593.0|
     * |103493|Bordesley Green G...|   Secondary|Does not apply|          Birmingham|     990|  B9 4TR|                    3|Birmingham, Ladywood| Girls|Requires improvement|                               10|                       9593.0|
     * |103499|Turves Green Girl...|   Secondary|Does not apply|          Birmingham|     649| B31 4BP|                    2|Birmingham, North...| Girls|                Good|                               10|                       9593.0|
     * |138937|King Edward VI Ha...|   Secondary|Does not apply|          Birmingham|     957| B20 2HL|                    1|Birmingham, Perry...| Girls|         Outstanding|                               10|                       9593.0|
     * |103514|   Swanshurst School|   Secondary|Does not apply|          Birmingham|    1822| B13 0TW|                    1|Birmingham, Selly...| Girls|         Outstanding|                               10|                       9593.0|
     * |136590|Kings Norton Girl...|   Secondary|          None|          Birmingham|    1013| B30 1HW|                    1|Birmingham, Selly...| Girls|         Outstanding|                               10|                       9593.0|
     * |103498|Selly Park  Girls...|   Secondary|Does not apply|          Birmingham|     703| B29 7PH|                    1|Birmingham, Selly...| Girls|         Outstanding|                               10|                       9593.0|
     * |136778|Sutton Coldfield ...|   Secondary|Does not apply|          Birmingham|    1160| B73 5PT|                    1|    Sutton Coldfield| Girls|         Outstanding|                               10|                       9593.0|
     * |137006|Langley Park Scho...|   Secondary|          None|             Bromley|    1502| BR3 3BE|                    2|           Beckenham| Girls|                Good|                                5|                       6227.0|
     * |136709| Bullers Wood School|   Secondary|          None|             Bromley|    1576| BR7 5LJ|                    1|Bromley and Chisl...| Girls|         Outstanding|                                5|                       6227.0|
     * |136467|Chislehurst Schoo...|   Secondary|          None|             Bromley|    1071| BR7 6HE|                    2|Bromley and Chisl...| Girls|                Good|                                5|                       6227.0|
     * |137379|Harris Girls Acad...|   Secondary|          None|             Bromley|     978| BR3 1QR|                    1|Lewisham West and...| Girls|         Outstanding|                                5|                       6227.0|
     * |136551|Newstead Wood School|   Secondary|          None|             Bromley|    1100| BR6 9SA|                    1|           Orpington| Girls|         Outstanding|                                5|                       6227.0|
     * |137985|    Presdales School|   Secondary|Does not apply|       Hertfordshire|    1114|SG12 9NX|                    1|Hertford and Stor...| Girls|         Outstanding|                                5|                       5770.0|
     * |140786|The Hertfordshire...|   Secondary|          None|       Hertfordshire|    1267|CM23 5NJ|                    1|Hertford and Stor...| Girls|         Outstanding|                                5|                       5770.0|
     * |137288|Hitchin Girls' Sc...|   Secondary|Does not apply|       Hertfordshire|    1188| SG4 9RS|                    1|Hitchin and Harpe...| Girls|         Outstanding|                                5|                       5770.0|
     * |137339|St Albans Girls' ...|   Secondary|Does not apply|       Hertfordshire|    1306| AL3 6DB|                    1|           St Albans| Girls|         Outstanding|                                5|                       5770.0|
     * |137757|Bishop's Hatfield...|   Secondary|Does not apply|       Hertfordshire|     895|AL10 8NL|                    1|     Welwyn Hatfield| Girls|         Outstanding|                                5|                       5770.0|
     * |137630|Walderslade Girls...|   Secondary|Does not apply|              Medway|     865| ME5 0LE|                    2|Chatham and Ayles...| Girls|                Good|                                5|                       5396.0|
     * |137389|     Chatham Grammar|   Secondary|          None|              Medway|     804| ME5 7EH|                    2|Chatham and Ayles...| Girls|                Good|                                5|                       5396.0|
     * |136456|Rainham School fo...|   Secondary|Does not apply|              Medway|    1655| ME8 0BX|                    2|Gillingham and Ra...| Girls|                Good|                                5|                       5396.0|
     * |136337|Fort Pitt Grammar...|   Secondary|          None|              Medway|     836| ME4 6TJ|                    1|Rochester and Strood| Girls|         Outstanding|                                5|                       5396.0|
     * |136313|The Rochester Gra...|   Secondary|          None|              Medway|    1236| ME1 3BY|                    1|Rochester and Strood| Girls|         Outstanding|                                5|                       5396.0|
     * |137130|Prenton High Scho...|   Secondary|Does not apply|              Wirral|     773|CH42 6RR|                    2|          Birkenhead| Girls|                Good|                                5|                       5915.0|
     * |135877|Birkenhead High S...|   Secondary|Does not apply|              Wirral|    1146|CH43 1TY|                    1|          Birkenhead| Girls|         Outstanding|                                5|                       5915.0|
     * |137815|Weatherhead High ...|   Secondary|Does not apply|              Wirral|    1586|CH44 3HS|                    1|            Wallasey| Girls|         Outstanding|                                5|                       5915.0|
     * |137171|Wirral Grammar Sc...|   Secondary|Does not apply|              Wirral|    1183|CH63 3AF|                    1|        Wirral South| Girls|         Outstanding|                                5|                       5915.0|
     * |137243|West Kirby Gramma...|   Secondary|          None|              Wirral|    1227|CH48 5DP|                    1|         Wirral West| Girls|         Outstanding|                                5|                       5915.0|
     * |136846|Aylesbury High Sc...|   Secondary|Does not apply|     Buckinghamshire|    1291|HP21 7SX|                    1|           Aylesbury| Girls|         Outstanding|                                4|                       5014.0|
     * |140893|Beaconsfield High...|   Secondary|          None|     Buckinghamshire|    1199| HP9 1RR|                    1|        Beaconsfield| Girls|         Outstanding|                                4|                       5014.0|
     * |137219|Dr Challoner's Hi...|   Secondary|          None|     Buckinghamshire|    1216| HP7 9QB|                    1|Chesham and Amersham| Girls|         Outstanding|                                4|                       5014.0|
     * |136723| Wycombe High School|   Secondary|          None|     Buckinghamshire|    1308|HP11 1TB|                    1|             Wycombe| Girls|         Outstanding|                                4|                       5014.0|
     * |139140|  Boston High School|   Secondary|          None|        Lincolnshire|     799|PE21 9PF|                    2| Boston and Skegness| Girls|                Good|                                4|                       3749.0|
     * |138638|Kesteven and Gran...|   Secondary|Does not apply|        Lincolnshire|    1214|NG31 9AU|                    1|Grantham and Stam...| Girls|         Outstanding|                                4|                       3749.0|
     * |137667|Kesteven and Slea...|   Secondary|Does not apply|        Lincolnshire|     756|NG34 7RS|                    2|Sleaford and Nort...| Girls|                Good|                                4|                       3749.0|
     * |120642|Spalding High School|   Secondary|Does not apply|        Lincolnshire|     980|PE11 2PJ|                    1|South Holland and...| Girls|         Outstanding|                                4|                       3749.0|
     * |137131|Queen Elizabeth's...|   Secondary|Does not apply|              Barnet|    1040| EN5 5RR|                    2|     Chipping Barnet| Girls|                Good|                                3|                       2752.0|
     * |138051|The Henrietta Bar...|   Secondary|          None|              Barnet|     792|NW11 7BN|                    1|Finchley and Gold...| Girls|         Outstanding|                                3|                       2752.0|
     * |138685|     Copthall School|   Secondary|Does not apply|              Barnet|     920| NW7 2EP|                    2|              Hendon| Girls|                Good|                                3|                       2752.0|
     * |136996|Bournemouth Schoo...|   Secondary|          None|Bournemouth, Chri...|    1159| BH8 9UJ|                    1|    Bournemouth East| Girls|         Outstanding|                                3|                       3245.0|
     * |140008|    Glenmoor Academy|   Secondary|          None|Bournemouth, Chri...|     862|BH10 4EX|                    1|    Bournemouth West| Girls|         Outstanding|                                3|                       3245.0|
     * |136368|Parkstone Grammar...|   Secondary|          None|Bournemouth, Chri...|    1224|BH17 7EP|                    1|               Poole| Girls|         Outstanding|                                3|                       3245.0|
     * |147067|Bronte Girls' Aca...|   Secondary|          None|            Bradford|     127| BD4 7EB|                 NULL|       Bradford East| Girls|Requires improvement|                                3|                       2305.0|
     * |138087|Belle Vue Girls' ...|   Secondary|Does not apply|            Bradford|    1130| BD9 6NA|                    2|       Bradford West| Girls|                Good|                                3|                       2305.0|
     * |140204|Bradford Girls' G...|   Secondary|          None|            Bradford|    1048| BD9 6RB|                    4|       Bradford West| Girls|Requires improvement|                                3|                       2305.0|
     * |136767|Ribston Hall High...|   Secondary|          None|     Gloucestershire|     816| GL1 5LE|                    2|          Gloucester| Girls|                Good|                                3|                       2763.0|
     * |136666|Denmark Road High...|   Secondary|          None|     Gloucestershire|     916| GL1 3JN|                    1|          Gloucester| Girls|         Outstanding|                                3|                       2763.0|
     * |136874|  Stroud High School|   Secondary|          None|     Gloucestershire|    1031| GL5 4HF|                    1|              Stroud| Girls|         Outstanding|                                3|                       2763.0|
     * |137060|Tolworth Girls' S...|   Secondary|          None|Kingston upon Thames|    1375| KT6 7EY|                    1|Kingston and Surb...| Girls|         Outstanding|                                3|                       3940.0|
     * |136615|The Tiffin Girls'...|   Secondary|          None|Kingston upon Thames|    1190| KT2 5PL|                    1|       Richmond Park| Girls|         Outstanding|                                3|                       3940.0|
     * |137848|Coombe Girls' School|   Secondary|Does not apply|Kingston upon Thames|    1375| KT3 3TU|                    1|       Richmond Park| Girls|         Outstanding|                                3|                       3940.0|
     * |135174|The Belvedere Aca...|   Secondary|Does not apply|           Liverpool|     949|  L8 3TF|                    1|Liverpool, Riverside| Girls|         Outstanding|                                3|                       1837.0|
     * |148226|   Alsop High School|   Secondary|Does not apply|           Liverpool|    NULL|  L4 6SH|                    4|   Liverpool, Walton| Girls|Requires improvement|                                3|                       1837.0|
     * |104688|Holly Lodge Girls...|   Secondary|Does not apply|           Liverpool|     888| L12 7LE|                    2|Liverpool, West D...| Girls|                Good|                                3|                       1837.0|
     * |136797|Carshalton High S...|   Secondary|Does not apply|              Sutton|    1344| SM5 2QX|                    2|Carshalton and Wa...| Girls|                Good|                                3|                       4267.0|
     * |136789|Wallington High S...|   Secondary|          None|              Sutton|    1497| SM6 0PH|                    2|Carshalton and Wa...| Girls|                Good|                                3|                       4267.0|
     * |136795|Nonsuch High Scho...|   Secondary|          None|              Sutton|    1426| SM3 8AB|                    2|     Epsom and Ewell| Girls|                Good|                                3|                       4267.0|
     * |137769|Townley Grammar S...|   Secondary|Does not apply|              Bexley|    1633| DA6 7AB|                    1|Bexleyheath and C...| Girls|         Outstanding|                                2|                       3000.0|
     * |137965|Blackfen School f...|   Secondary|Does not apply|              Bexley|    1367|DA15 9NU|                    2|Old Bexley and Si...| Girls|                Good|                                2|                       3000.0|
     * |100050|Parliament Hill S...|   Secondary|Does not apply|              Camden|    1166| NW5 1RL|                    2|Holborn and St Pa...| Girls|                Good|                                2|                       2209.0|
     * |100054|The Camden School...|   Secondary|          None|              Camden|    1043| NW5 2DB|                    1|Holborn and St Pa...| Girls|         Outstanding|                                2|                       2209.0|
     * |136412|Chelmsford County...|   Secondary|          None|               Essex|     987| CM1 1RW|                    1|          Chelmsford| Girls|         Outstanding|                                2|                       2018.0|
     * |137515|Colchester County...|   Secondary|          None|               Essex|    1031| CO3 3US|                    1|          Colchester| Girls|         Outstanding|                                2|                       2018.0|
     * |100182|  Eltham Hill School|   Secondary|Does not apply|           Greenwich|    1153| SE9 5EE|                    2|              Eltham| Girls|                Good|                                2|                       1385.0|
     * |143686|Woolwich Polytech...|   Secondary|          None|           Greenwich|     232|SE28 8RF|                 NULL|Erith and Thamesmead| Girls|Requires improvement|                                2|                       1385.0|
     * |100455|Highbury Fields S...|   Secondary|Does not apply|           Islington|     762|  N5 1AR|                    1|     Islington North| Girls|         Outstanding|                                2|                       1654.0|
     * |100457|Elizabeth Garrett...|   Secondary|Does not apply|           Islington|     892|  N1 9QG|                    1|Islington South a...| Girls|         Outstanding|                                2|                       1654.0|
     * |136381|Lancaster Girls' ...|   Secondary|          None|          Lancashire|     958| LA1 1SF|                    1|Lancaster and Fle...| Girls|         Outstanding|                                2|                       1727.0|
     * |119765|Penwortham Girls'...|   Secondary|Does not apply|          Lancashire|     769| PR1 0SR|                    1|        South Ribble| Girls|         Outstanding|                                2|                       1727.0|
     * |100741|     Sydenham School|   Secondary|Does not apply|            Lewisham|    1412|SE26 4RD|                    2|Lewisham West and...| Girls|                Good|                                2|                       2301.0|
     * |100750|  Prendergast School|   Secondary|          None|            Lewisham|     889| SE4 1LE|                    1|  Lewisham, Deptford| Girls|         Outstanding|                                2|                       2301.0|
     * |141264|Whalley Range 11-...|   Secondary|Does not apply|          Manchester|    1569| M16 8GW|                    2|  Manchester, Gorton| Girls|                Good|                                2|                       2572.0|
     * |141196|Levenshulme High ...|   Secondary|Does not apply|          Manchester|    1003| M19 1FS|                    1|  Manchester, Gorton| Girls|         Outstanding|                                2|                       2572.0|
     * |102782|      Plashet School|   Secondary|Does not apply|              Newham|    1434|  E6 1DG|                    1|            East Ham| Girls|         Outstanding|                                2|                       2715.0|
     * |142644|Sarah Bonnell School|   Secondary|Does not apply|              Newham|    1281| E15 4LP|                    2|            West Ham| Girls|                Good|                                2|                       2715.0|
     * |136976|Southfield School...|   Secondary|          None|    Northamptonshire|    1070|NN15 6HE|                    2|           Kettering| Girls|                Good|                                2|                       2802.0|
     * |140510|Northampton Schoo...|   Secondary|          None|    Northamptonshire|    1732| NN3 6DG|                    1|   Northampton North| Girls|         Outstanding|                                2|                       2802.0|
     * |113532|Plymouth High Sch...|   Secondary|Does not apply|            Plymouth|     794| PL4 6HT|                    2|Plymouth, Sutton ...| Girls|                Good|                                2|                       1642.0|
     * |136588|Devonport High Sc...|   Secondary|Does not apply|            Plymouth|     848| PL2 3DL|                    2|Plymouth, Sutton ...| Girls|                Good|                                2|                       1642.0|
     * |144610|Reading Girls' Sc...|   Secondary|          None|             Reading|     474| RG2 7PY|                    4|        Reading East| Girls|Requires improvement|                                2|                       1227.0|
     * |136448|     Kendrick School|   Secondary|          None|             Reading|     753| RG1 5BN|                    1|        Reading East| Girls|         Outstanding|                                2|                       1227.0|
     * |136444|Southend High Sch...|   Secondary|          None|     Southend on Sea|    1178| SS2 4UZ|                    1|Rochford and Sout...| Girls|         Outstanding|                                2|                       2447.0|
     * |136490|Westcliff High Sc...|   Secondary|          None|     Southend on Sea|    1269| SS0 0BS|                    1|       Southend West| Girls|         Outstanding|                                2|                       2447.0|
     * |131747|Harris Academy Be...|   Secondary|Does not apply|           Southwark|     851|SE16 3TZ|                    1|Bermondsey and Ol...| Girls|         Outstanding|                                2|                       1712.0|
     * |132711|Harris Girls' Aca...|   Secondary|Does not apply|           Southwark|     861|SE22 0NR|                    1|Camberwell and Pe...| Girls|         Outstanding|                                2|                       1712.0|
     * |100975|Central Foundatio...|   Secondary|          None|       Tower Hamlets|    1513|  E3 2AE|                    2|Bethnal Green and...| Girls|                Good|                                2|                       2945.0|
     * |143629|Mulberry School f...|   Secondary|Does not apply|       Tower Hamlets|    1432|  E1 2JP|                    1|Poplar and Limehouse| Girls|         Outstanding|                                2|                       2945.0|
     * |137289|Altrincham Gramma...|   Secondary|          None|            Trafford|    1363|WA14 2NL|                    1|Altrincham and Sa...| Girls|         Outstanding|                                2|                       2306.0|
     * |136965|Flixton Girls School|   Secondary|          None|            Trafford|     943| M41 5DR|                    2|Stretford and Urm...| Girls|                Good|                                2|                       2306.0|
     * |139293|Connaught School ...|   Secondary|Does not apply|      Waltham Forest|     627| E11 4AE|                    2| Leyton and Wanstead| Girls|                Good|                                2|                       1522.0|
     * |103103|Walthamstow Schoo...|   Secondary|Does not apply|      Waltham Forest|     895| E17 9RZ|                    1|         Walthamstow| Girls|         Outstanding|                                2|                       1522.0|
     * |136595|   Rugby High School|   Secondary|          None|        Warwickshire|     895|CV22 7RE|                    1|               Rugby| Girls|         Outstanding|                                2|                       1685.0|
     * |137235|Stratford Girls' ...|   Secondary|Does not apply|        Warwickshire|     790|CV37 9HA|                    1|   Stratford-on-Avon| Girls|         Outstanding|                                2|                       1685.0|
     * |142357|Newlands Girls' S...|   Secondary|Does not apply|Windsor and Maide...|    1150| SL6 5JB|                    1|          Maidenhead| Girls|         Outstanding|                                2|                       1925.0|
     * |141852|Windsor Girls' Sc...|   Secondary|Does not apply|Windsor and Maide...|     775| SL4 3RT|                    1|             Windsor| Girls|         Outstanding|                                2|                       1925.0|
     * +------+--------------------+------------+--------------+--------------------+--------+--------+---------------------+--------------------+------+--------------------+---------------------------------+-----------------------------+
     **/

  }

}
