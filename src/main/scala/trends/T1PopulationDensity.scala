package trends

import covid.tables.DFTables
import trends.util.{DateValDiff,CovidRowFunctions,RowFunctionBase}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark.storage.StorageLevel

object PopulationDensity {
    def deflateDFTable: Unit = {
        val columns     = DFTables.getHeaderCOVID_19Confirmed
        val cov_19Conf  = DFTables.getCOVID_19Confirmed.rdd
        
        val filteredRDD = filterCountryOfInterest(cov_19Conf)
        val fltGrpFilRDD = groupByCountryAndSumNumFields(filteredRDD, columns)

        val selColumns = selectColumns(columns)
        val selFltGrpFilRDD = dropExtraColumns(fltGrpFilRDD, selColumns)
        val filteredDiffRDD = calculateDailyDifference(selFltGrpFilRDD, selColumns).persist(StorageLevel.MEMORY_ONLY)
        
        val lowMedHighDen = getLowMedHighDensityRDDs(filteredDiffRDD)
        
        showLowMedHigh(lowMedHighDen, selColumns)

        filteredDiffRDD.unpersist()
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

    def createBaseRowForAggregation(sampleRow: Row): Row = {
        Row.fromSeq(Row.unapplySeq(sampleRow).get.map{
            case ele if(ele == null) => null
            case (ele: Int)          => 0.toLong
            case (ele: Long)         => 0l
            case (ele: Double)       => 0.0
            case _                   => ""
        })
    }

    def sumRowElements(lftRow: Row, rtRow: Row): Row = {
        Row.fromSeq(Row.unapplySeq(lftRow).get.zip(Row.unapplySeq(rtRow).get).map {
            case ((el1: Long), (el2: Long))     => el1 + el2
            case ((el1: Double), (el2: Double)) => el1 + el2
            case (el1, el2) if (el1 == null)    => el2
            case (el1, el2) if (el2 == null)    => null
            case (_, el2)               => el2.toString
        })
    }

    def selectColumns(cols: Seq[String]): Seq[String] = {
        cols.filter(col => CovidRowFunctions.isDateCol(col) || col == "Country/Region")
    }

    def filterCountryOfInterest(rdd_handle: RDD[Row]): RDD[Row] = {
        rdd_handle.filter(row => isCountryOfInterest(row.getAs[String]("Country/Region")))
    }

    def groupByCountryAndSumNumFields(rdd_handle: RDD[Row], cols: Seq[String]): RDD[Row] = {
        val base_case_Row = createBaseRowForAggregation(rdd_handle.first)
        val grpFilRDD = rdd_handle.groupBy(row => row.getAs[String]("Country/Region"))
                        .aggregateByKey(base_case_Row)(
                            (baseRow,itrRow) => itrRow.foldLeft(baseRow)((f,v) => sumRowElements(f,v)),
                            (baseRow,summedRow) => summedRow
                        )
        RowFunctionBase.inferPrimSchema(grpFilRDD.map{case (_, row) => row} -> cols).rdd
    }

    def dropExtraColumns(rdd_handle: RDD[Row], cols: Seq[String]): RDD[Row] = {
        RowFunctionBase.createDataFrame(rdd_handle -> cols)._1.rdd
    }

    def calculateDailyDifference(rdd_handle: RDD[Row], cols: Seq[String]): RDD[Row] = {
        DateValDiff.divideDiffRDD(rdd_handle -> cols)._1
    }

    def getLowMedHighDensityRDDs(rdd_handle: RDD[Row]): Tuple3[RDD[Row],RDD[Row],RDD[Row]] = {
        val lowDenFil  = rdd_handle.filter(row => isLowDensCo(row.getAs[String]("Country/Region")))
        val medDenFil  = rdd_handle.filter(row => isMedDensCo(row.getAs[String]("Country/Region")))
        val highDenFil = rdd_handle.filter(row => isHighDensCo(row.getAs[String]("Country/Region")))
        (lowDenFil,medDenFil,highDenFil)
    }

    def showLowMedHigh(rrr: Tuple3[RDD[Row],RDD[Row],RDD[Row]], cols: Seq[String]): Unit = rrr match{
        case (lowDenFil, medDenFil, highDenFil) =>
            RowFunctionBase.inferPrimSchema(lowDenFil -> cols).show()
            RowFunctionBase.inferPrimSchema(medDenFil -> cols).show()
            RowFunctionBase.inferPrimSchema(highDenFil -> cols).show()
    }
}