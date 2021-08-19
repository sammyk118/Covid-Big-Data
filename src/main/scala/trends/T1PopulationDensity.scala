package trends

import session.spark.LocalSparkSession
import covid.tables.DFTables
import trends.util.DateValDiff

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
/*
 *  Work in Progress
 */

object PopulationDensity {
    def deflateDFTable: Unit = {
        val spark      = LocalSparkSession()
        val columns    = DFTables.getHeaderCOVID_19Confirmed
        val cov_19Conf = DFTables.getCOVID_19Confirmed.rdd

        val filtered = cov_19Conf.filter(row => isCountryOfInterest(row.getAs[String]("Country/Region")))

        //DateValDiff.divideDiffRDD(filtered -> columns)._1.foreach(println(_))
        filtered.foreach(x => println(x))
        trends.util.DateMax.findMaxIncludeCountryAndDate(DateValDiff.divideDiffDF(DFTables.getCOVID_19Confirmed)).where("Max < 1500").show(300)
    }

    def isCountryOfInterest(country: String): Boolean = {
        country == "US" || country == "Spain" || country == "United Kingdom" || country == "UK" ||
            country == "Korea, South" || country == "Nepal" || country == "Philippines" || country == "India" ||
            country == "Afghanistan" || country == "France" || country == "Russia" || country == "China"
    }
}