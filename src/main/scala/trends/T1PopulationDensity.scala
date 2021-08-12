package trends

import session.spark.LocalSparkSession
import covid.tables.DFTables
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.rdd.RDD

/*
 *  Work in Progress
 */

object PopulationDensity {
    def deflateDFTable: Unit = {
        val spark      = LocalSparkSession()
        val columns    = DFTables.getHeaderCOVID_19Confirmed
        val cov_19Conf = DFTables.getCOVID_19Confirmed.rdd

        import spark.sql
        import spark.implicits._

        val bColumns   = spark.sparkContext.broadcast(columns)

        val filtered = cov_19Conf.filter(row => isCountryOfInterest(row.getAs[String]("Country/Region")))
        
        DFTables.getCOVID_19Confirmed.createTempView("temp")
        sql("Select Sum( lat ) as Total from temp").show()
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