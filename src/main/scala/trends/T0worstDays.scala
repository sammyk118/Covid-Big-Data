package trends

import trends.util.DateValDiff
import covid.tables.DFTables
import trends.util.DateMax
import session.spark.LocalSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object T0worstDays {
  def findMax: Unit = {

    val confirmed = DFTables.getCOVID_19Confirmed
    val deaths = DFTables.getCOVID_19Deaths
    val recovered = DFTables.getCOVID_19Recovered
    val rename: Seq[(String, String)] => Seq[String] = columNames => columNames.map { case (col1, _) => if (col1.contains("sum")) col1.split("\\(")(1).init else col1 }


    val caseDay = DateMax.findMaxIncludeCountryAndDate(
      DateValDiff.divideDiffDF(
        groupCountries(confirmed), rename)
    )
    val deathDay = DateMax.findMaxIncludeCountryAndDate(
      DateValDiff.divideDiffDF(
        groupCountries(deaths), rename)
    )
    val recoverDay = DateMax.findMaxIncludeCountryAndDate(
      DateValDiff.divideDiffDF(
        groupCountries(recovered), rename)
    )
    val filtCase = caseDay.filter(row => filteredCountry(row.getAs[String]("Country/Region")))
    println("Cases: ")
    filtCase.show()

    val filtRec = caseDay.filter(row => filteredCountry(row.getAs[String]("Country/Region")))
    println("Recovered: ")
    filtRec.show()

    val filtDeath = caseDay.filter(row => filteredCountry(row.getAs[String]("Country/Region")))
    println("Deaths: ")
    filtDeath.show()


    println("Cases:")
    caseDay.orderBy(desc("Max")).show()
    println("Deaths:")
    deathDay.orderBy(desc("Max")).show()
    println("Recovered:")
    recoverDay.orderBy(desc("Max")).show()
  }


  val spark = LocalSparkSession()

  def filteredCountry(country: String): Boolean = {
    country == "US" || country == "Germany" || country == "United Kingdom" || country == "Turkey" || country == "India" || country == "Brazil" || country == "Poland" || country == "Italy" || country == "Russia" || country == "South Africa"
  }

  def groupCountries(table: DataFrame): DataFrame = {
    table.groupBy("Country/Region").sum()
  }
}