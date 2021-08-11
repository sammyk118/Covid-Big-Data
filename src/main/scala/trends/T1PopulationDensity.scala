package trends

import covid.tables.DFTables
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD

/*
 *  Work in Progress
 */

object PopulationDensity {
    def deflateDFTable: RDD[Row] = {
        val columns    = DFTables.getHeaderCOVID_19Confirmed
        val cov_19Conf = DFTables.getCOVID_19Confirmed.rdd

        val filtered = cov_19Conf.filter(row => isCountryOfInterest(row.getAs[String]("Country/Region")))

        filtered.mapPartitions[Row](rIter => columns
            .map(col => rIter.map(row => Row((col, row.getAs[String](col)) ))).toIterator.flatten)
    }

    def isCountryOfInterest(country: String): Boolean = {
        country == "US" || country == "Spain" || country == "United Kingdom" || country == "UK" ||
            country == "Korea" || country == "Nepal" || country == "Philippines" || country == "India"
    }

    def main(args: Array[String]): Unit = {
        deflateDFTable.collect().foreach(println)
    }
}