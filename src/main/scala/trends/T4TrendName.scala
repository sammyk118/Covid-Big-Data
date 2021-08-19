package trends

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.col
import session.spark.LocalSparkSession
import covid.tables.DFTables
import trends.util.DateValDiff


object TrendFour{
  def start(): Unit ={
    val spark = LocalSparkSession()
//    spark.sql("select * from recPct").show()
    spark.sql("select `Country/Region`, (totalRecovered/totalCases)*100 as percent_recovered from recPct " +
      "where `Country/Region` = 'Afghanistan' " +
      "or `Country/Region` = 'Estonia' " +
      "or `Country/Region` = 'Ghana' " +
      "or `Country/Region` = 'Philippines' " +
      "or `Country/Region` = 'Venezuela' " +
      "or `Country/Region` = 'Cambodia'").show()
    spark.sql("select `Country/Region`, (totalRecovered/totalCases)*100 as percent_recovered from recPct " +
      "where `Country/Region` = 'Italy' " +
      "or `Country/Region` = 'Germany' " +
      "or `Country/Region` = 'Japan' " +
      "or `Country/Region` = 'Korea, South' " +
      "or `Country/Region` = 'Luxembourg' " +
      "or `Country/Region` = 'United Arab Emirates'").show()
//    val covid19DataDF = DFTables.getCOVID_19Data
//    covid19DataDF.createOrReplaceTempView("Covid19Data")
//    spark.sql("select `Country/Region`, (max(Recovered)/max(Confirmed))*100 as rateOfRecovery from Covid19Data where `Country/Region` = 'Germany' " +
//      "or `Country/Region` = 'France' " +
//      "or `Country/Region` = 'UK' " +
//      "group by `Country/Region` ").show()
    //spark.sql("select sum(`5/2/21`) as `5/2/21` from usCasesTable").createOrReplaceTempView("totalUSCases")

    //spark.sql("select * from casesTable").show()

    //spark.sql("select recoveredTable.`Country/Region`, (recoveredTable.`5/2/21`/casesTable.`5/2/21`)*100 as percentage " +
      //"from recoveredTable join casesTable on casesTable.Lat =  recoveredTable.Lat").show()
  }
}