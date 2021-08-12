package covid.tables

import org.apache.spark.sql.{SparkSession, DataFrame}
import session.spark.LocalSparkSession
import org.apache.calcite.interpreter.Row
import org.apache.spark.sql.DataFrameReader

object DFTables {
    private val c19DataRP       = "input/covid_19_data.csv"
    private val tsc19Conf       = "input/time_series_covid_19_confirmed.csv"
    private val tsc19ConfUS     = "input/time_series_covid_19_confirmed_US.csv"
    private val tsc19Deaths     = "input/time_series_covid_19_deaths.csv"
    private val tsc19DeathsUS   = "input/time_series_covid_19_deaths_US.csv"
    private val tsc19Recovered  = "input/time_series_covid_19_recovered.csv"

    private var dataCache       : DataFrame = null
    private var confCache       : DataFrame = null
    private var confUSCache     : DataFrame = null
    private var deathsCache     : DataFrame = null
    private var deathsUSCache   : DataFrame = null
    private var recoveredCache  : DataFrame = null

    private val lss             = LocalSparkSession()

    def getCOVID_19Data: DataFrame = if (dataCache == null) {dataCache = createDFTable(c19DataRP).cache(); dataCache} else dataCache

    def getCOVID_19Confirmed: DataFrame = if (confCache == null) {confCache = createDFTable(tsc19Conf).cache(); confCache} else confCache

    def getCOVID_19ConfirmedUS: DataFrame = if (confUSCache == null) {confUSCache = createDFTable(tsc19ConfUS).cache(); confUSCache} else confUSCache

    def getCOVID_19Deaths: DataFrame = if (deathsCache == null) {deathsCache = createDFTable(tsc19Deaths).cache(); deathsCache} else deathsCache

    def getCOVID_19DeathsUS: DataFrame = if (deathsUSCache == null) {deathsUSCache = createDFTable(tsc19DeathsUS).cache(); deathsUSCache} else deathsUSCache

    def getCOVID_19Recovered: DataFrame = if (recoveredCache == null) {recoveredCache = createDFTable(tsc19Recovered).cache(); recoveredCache} else recoveredCache


    // The header methods are probably not necessary, but I've included them in the off chance that they could be of convenience

    def getHeaderCOVID_19Data: Array[String] = getCOVID_19Data.columns

    def getHeaderCOVID_19Confirmed: Array[String] = getCOVID_19Confirmed.columns

    def getHeaderCOVID_19ConfirmedUS: Array[String] = getCOVID_19ConfirmedUS.columns

    def getHeaderCOVID_19Deaths: Array[String] = getCOVID_19Deaths.columns

    def getHeaderCOVID_19DeathsUS: Array[String] = getCOVID_19DeathsUS.columns

    def getHeaderCOVID_19Recovered: Array[String] = getCOVID_19Recovered.columns
    

    private def createDFTable(tablePath: String): DataFrame = lss.read.options(Map("inferSchema" -> "true", "header" -> "true")).csv(tablePath)
}