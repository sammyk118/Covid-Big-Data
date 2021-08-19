package trends

import session.spark.LocalSparkSession
import covid.tables.DFTables
import trends.util.DateValDiff

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Column,Row,RelationalGroupedDataset}
import org.apache.spark.sql.functions. {sum,col}
import trends.util.CovidRowFunctions
import trends.util.RowFunctionBase
/*
 *  Work in Progress
 */

object PopulationDensity {
    def deflateDFTable: Unit = {
        val spark      = LocalSparkSession()
        val columns    = DFTables.getHeaderCOVID_19Confirmed
        val cov_19Conf = DFTables.getCOVID_19Confirmed.rdd

        val filtered = cov_19Conf.filter(row => isCountryOfInterest(row.getAs[String]("Country/Region")))

        val filteredDF = RowFunctionBase.inferPrimSchema(filtered -> columns)
        filteredDF.show()
        //DateValDiff.divideDiffDF(filteredDF).show()
        // val base_seq = createBaseSeqForSum(filtered.first)
        // filtered.groupBy(row => row(1)).aggregateByKey(base_seq)

        //println(filtered.first.)

        //DateValDiff.divideDiffRDD(filtered -> columns)._1.foreach(println(_))
    }

    def isCountryOfInterest(country: String): Boolean = {
        country == "US" || country == "Spain" || country == "United Kingdom" || country == "UK" ||
            country == "Korea, South" || country == "Nepal" || country == "Philippines" || country == "India" ||
            country == "Afghanistan" || country == "France" || country == "Russia" || country == "China"
    }

    // def createBaseSeqForSum(row: Row): Seq[Any] = {
    //     row.schema.head.metadata.
    // }
}