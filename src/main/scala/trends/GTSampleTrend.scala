package trends

import trends.util.DateValDiff
import covid.tables.DFTables
import trends.util.DateMax
import session.spark.LocalSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}

object T0worstDays {
  def findMax: Unit = {
    val spark = LocalSparkSession()
    import spark.implicits._

    // dates and country -> use createsumselect from t2, add sum to every date col. no formatcolumnexplicit
    val confirmed = DFTables.getCOVID_19Confirmed
    println("hooray!")
    val aggDF = groupCountries(confirmed)
    aggDF.show()
//    val filtered = confirmed.filter(row => filteredCountry(row.getAs[String]("Country/Region")))
    val conDiff = DateValDiff.divideDiffDF(aggDF)
    conDiff.cache()
    conDiff.show()
    val maxDays = DateMax.findMaxIncludeCountryAndDate(conDiff)
    maxDays.show()



//    val maxrdd = cDiffNeatRDD.map(row => {
//      count += 1
//      row.toSeq.fold(0)((acc, ele) => {
//        acc.toString.toInt max ele.toString.toInt
//      }
//      )
//    })
//    val maxDays = DateMax.findMaxIncludeCountryAndDate(conDiff)
//    maxDays.show()

  }

  val spark = LocalSparkSession()


  def filteredCountry(country: String): Boolean = {
    country == "US" || country == "Germany" || country == "United Kingdom"
  }

  def groupCountries(table: DataFrame): DataFrame = {
    table.groupBy("Country/Region").sum()
  }
}