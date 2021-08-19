package trends

import session.spark.LocalSparkSession
import covid.tables.DFTables
import trends.util.DateValDiff
import trends.util.CovidRowFunctions
import trends.util.RowFunctionBase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Column,Row}
import org.apache.spark.storage.StorageLevel
/*
 *  Work in Progress
 */

object PopulationDensity {
    def deflateDFTable: Unit = {
        val spark                = LocalSparkSession()
        val columns: Seq[String] = DFTables.getHeaderCOVID_19Confirmed
        val cov_19Conf           = DFTables.getCOVID_19Confirmed

        val filteredDF0 = cov_19Conf.filter(row => isCountryOfInterest(row.getAs[String]("Country/Region"))).drop("Lat","Long")
        val filteredDF = filteredDF0.groupBy("Country/Region").sum().drop("sum(Lat)","sum(Long)").persist(StorageLevel.MEMORY_ONLY)

        val filteredDiffDF = DateValDiff.divideDiffDF(filteredDF, renameColumns _)

        val lowDenFil  = filteredDiffDF.filter(row => isLowDensCo(row.getAs[String]("Country/Region")))
        val medDenFil  = filteredDiffDF.filter(row => isMedDensCo(row.getAs[String]("Country/Region")))
        val highDenFil = filteredDiffDF.filter(row => isHighDensCo(row.getAs[String]("Country/Region")))

        lowDenFil.show()
        medDenFil.show()
        highDenFil.show()
    }

    def isCountryOfInterest(country: String): Boolean = {
        country == "Spain" || country == "United Kingdom" || country == "UK" ||
            country == "Korea, South" || country == "Nepal" || country == "Philippines" || country == "India" ||
            country == "Afghanistan" || country == "France" || country == "Russia" || country == "China"
        
    }

    def isLowDensCo(country: String): Boolean = {
        country == "Afghanistan" || country == "Russia"
    }

    def isMedDensCo(country: String): Boolean = {
        country == "France" || country == "Spain" || country == "Nepal" || country == "China"
    }

    def isHighDensCo(country: String): Boolean = {
        country == "India" || country == "Philippines" || country == "Korea, South" || country == "Japan"
    }

    def renameColumns(oldCols: Seq[(String,String)]): Seq[String] = oldCols.map {
        case (col,_) =>
            if (col.contains("sum"))
                col.split("\\(").last.init
            else
                col 
    }
}