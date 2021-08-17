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

        //val ((othColsDF, colNames), (valColsDF, diffColNames)) = divideDiffRDD(filtered, columns)
        // val othCols = othColsDF.rdd
        // val valCols = valColsDF.rdd

        //valCols.foreach(row => println(row))

        DateValDiff.divideDiffDF(DFTables.getCOVID_19Confirmed).show(5, 7, false)
        
        /* 
        filtered.mapPartitions[Row](rIter => bColumns.value
            .map(col => rIter.map(row => Row(col) +: Row(row.getAs[String](col)) ))).toIterator.flatten).coalesce(1,true)
        */
    }

    def isCountryOfInterest(country: String): Boolean = {
        country == "US" || country == "Spain" || country == "United Kingdom" || country == "UK" ||
            country == "Korea" || country == "Nepal" || country == "Philippines" || country == "India"
    }
}