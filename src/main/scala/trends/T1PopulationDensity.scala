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

        DateValDiff.divideDiffRDD(filtered -> columns)._1.foreach(println(_))
    }

    def isCountryOfInterest(country: String): Boolean = {
        country == "US" || country == "Spain" || country == "United Kingdom" || country == "UK" ||
            country == "Korea" || country == "Nepal" || country == "Philippines" || country == "India"
    }
}