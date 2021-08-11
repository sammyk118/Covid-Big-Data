package covid.tables

import org.apache.spark.sql.{SparkSession, DataFrame}
import session.spark.LocalSparkSession

object DFTables {
    private val c19DataRP       = "input/covid_19_data.csv"
    private val tsc19Conf       = "input/time_series_covid_19_confirmed.csv"
    private val tsc19ConfUS     = "input/time_series_covid_19_confirmed_US.csv"
    private val tsc19Deaths     = "input/time_series_covid_19_deaths.csv"
    private val tsc19DeathsUS   = "input/time_series_covid_19_deaths_US.csv"
    private val tsc19Recovered  = "input/time_series_covid_19_recovered.csv"

    private val lss             = LocalSparkSession()

    def getCOVID_19Data: DataFrame = lss.read.option("header", true).csv(c19DataRP)

    def getCOVID_19Confirmed: DataFrame = lss.read.option("header", true).csv(tsc19Conf)

    def getCOVID_19ConfirmedUS: DataFrame = lss.read.option("header", true).csv(tsc19ConfUS)

    def getCOVID_19Deaths: DataFrame = lss.read.option("header", true).csv(tsc19Deaths)

    def getCOVID_19DeathsUS: DataFrame = lss.read.option("header", true).csv(tsc19DeathsUS)

    def getCOVID_19Recovered: DataFrame = lss.read.option("header", true).csv(tsc19Recovered)


    // The header methods are probably not necessary, but I've included them in the off chance that they could be of convenience

    def getHeaderCOVID_19Data: Array[String] = getCOVID_19Data.columns

    def getHeaderCOVID_19Confirmed: Array[String] = getCOVID_19Confirmed.columns

    def getHeaderCOVID_19ConfirmedUS: Array[String] = getCOVID_19ConfirmedUS.columns

    def getHeaderCOVID_19Deaths: Array[String] = getCOVID_19Deaths.columns

    def getHeaderCOVID_19DeathsUS: Array[String] = getCOVID_19DeathsUS.columns

    def getHeaderCOVID_19Recovered: Array[String] = getCOVID_19Recovered.columns
}