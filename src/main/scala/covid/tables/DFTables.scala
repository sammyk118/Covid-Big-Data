package covid.tables

import org.apache.spark.sql.{SparkSession, DataFrame}
import session.spark.LocalSparkSession

object DFTables {
    private var c19DataRP       = "input/covid_19_data.csv"
    private var tsc19Conf       = "input/time_series_covid_19_confirmed.csv"
    private var tsc19ConfUS     = "input/time_series_covid_19_confirmed_US.csv"
    private var tsc19Deaths     = "input/time_series_covid_19_deaths.csv"
    private var tsc19DeathsUS   = "input/time_series_covid_19_deaths_US.csv"
    private var tsc19Recovered  = "input/time_series_covid_19_recovered.csv"

    private var lss             = LocalSparkSession()

    def getCOVID_19Data: DataFrame = lss.read.option("header", true).csv(c19DataRP)

    def getCOVID_19Confirmed: DataFrame = lss.read.option("header", true).csv(tsc19Conf)

    def getCOVID_19ConfirmedUS: DataFrame = lss.read.option("header", true).csv(tsc19ConfUS)

    def getCOVID_19Deaths: DataFrame = lss.read.option("header", true).csv(tsc19Deaths)

    def getCOVID_19DeathsUS: DataFrame = lss.read.option("header", true).csv(tsc19DeathsUS)

    def getCOVID_19Recovered: DataFrame = lss.read.option("header", true).csv(tsc19Recovered)
}