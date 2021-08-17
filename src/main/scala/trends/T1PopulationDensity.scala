package trends

import session.spark.LocalSparkSession
import covid.tables.DFTables
import trends.util.DateValDiff
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
/*
 *  Work in Progress
 */

object PopulationDensity {
    def deflateDFTable: Unit = {
        val columns    = DFTables.getHeaderCOVID_19Confirmed
        val cov_19Conf = DFTables.getCOVID_19Confirmed.rdd

        val filtered = cov_19Conf.filter(row => isCountryOfInterest(row.getAs[String]("Country/Region")))
        val (diffTbl, newCols) = DateValDiff.divideDiffRDD(filtered -> columns)
        val ((_,_),(dTbl,_)) = DateValDiff.divideDiff(cov_19Conf-> columns)

        val t = dTbl.rdd.map(row => row.toSeq.fold(0)((f,v) => if(f.toString.toInt > v.toString.toInt) f.toString.toInt else v.toString.toInt))
        t.foreach(r => println(r))
    }

    def isCountryOfInterest(country: String): Boolean = {
        country == "US" || country == "Spain" || country == "United Kingdom" || country == "UK" ||
            country == "Korea" || country == "Nepal" || country == "Philippines" || country == "India"
    }
}