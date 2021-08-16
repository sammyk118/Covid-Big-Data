package trends

import session.spark.LocalSparkSession
import covid.tables.DFTables
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType, StructField, StructType}
/*
 *  Work in Progress
 */

object PopulationDensity {
    def deflateDFTable: Unit = {
        val spark      = LocalSparkSession()
        val columns    = DFTables.getHeaderCOVID_19Confirmed
        val cov_19Conf = DFTables.getCOVID_19Confirmed.rdd

        val filtered = cov_19Conf.filter(row => isCountryOfInterest(row.getAs[String]("Country/Region")))

        val (othColsDF, (valColsDF, diffColNames)) = divideDiffRDD(filtered, columns)
        val othCols = othColsDF.rdd
        val valCols = valColsDF.rdd

        valCols.foreach(row => println(row))
        
        /* 
        filtered.mapPartitions[Row](rIter => bColumns.value
            .map(col => rIter.map(row => Row(col) +: Row(row.getAs[String](col)) ))).toIterator.flatten).coalesce(1,true)
        */
    }

    def isCountryOfInterest(country: String): Boolean = {
        country == "US" || country == "Spain" || country == "United Kingdom" || country == "UK" ||
            country == "Korea" || country == "Nepal" || country == "Philippines" || country == "India"
    }

    def divideDiffDF: DataFrame => Tuple2[DataFrame,(DataFrame, Seq[String])] = df_handle => {
        // val lss = LocalSparkSession()
        // import lss.implicits._
        divideDiffRDD(df_handle.rdd -> df_handle.columns)

        // othCols.rdd.zipWithIndex()
        // othCols
    }

    def divideDiffRDD: Tuple2[RDD[Row],Seq[String]] => Tuple2[DataFrame,(DataFrame, Seq[String])] = {
        case (rdd_handle, columns) => 
            val dateCols  = columns.filter(col => "\\d".r.findFirstIn(col).isDefined)
            val otherCols = columns.filter(col => "\\d".r.findFirstIn(col).isEmpty)
            val priOperandDate = dateCols.drop(1)
            val secOperandDate = dateCols.dropRight(1)

            (createDataFrame(rdd_handle -> otherCols), diffRDD(rdd_handle -> priOperandDate.zip(secOperandDate)))
    }

    def createRDD: Tuple2[RDD[Row], Seq[String]] => RDD[Row] = rc => createDataFrame(rc).rdd

    def createDataFrame: Tuple2[RDD[Row],Seq[String]] => DataFrame = {
        case (rdd_handle, columns) =>
            val unstructRDD = rdd_handle.map(row => Row(columns
                .foldLeft(Seq[Any]())((f, v) => f ++ Seq[Any](row.getAs[Any](v))):_*))
            
            inferPrimSchema(unstructRDD -> columns)
    }

    def inferPrimSchema: Tuple2[RDD[Row],Seq[String]] => DataFrame = {
        case (rdd_handle, columns) =>
            val keyVal = columns.zip(rdd_handle.take(1)(0).toSeq)
            val structType = StructType(keyVal.map{case (col,value) => StructField(col,inferType(value),true)})

            LocalSparkSession().createDataFrame(rdd_handle,structType)
    }

    def inferType: Any => DataType = {
        case null => StringType
        case n if(n.isInstanceOf[Int]) => IntegerType
        case n if(n.isInstanceOf[Double]) => DoubleType
        case s => StringType
    }

    def diffRDD: Tuple2[RDD[Row],Seq[(String,String)]] => (DataFrame, Seq[String]) = {
        case (rdd_handle, diffCols) =>
            val unstructRDD = rdd_handle.map(row => Row(diffCols
                .foldLeft(Seq[Int]())((f, v) => f ++ Seq[Int](row.getAs[Int](v._1) - row.getAs[Int](v._2))):_*))
            
            val defCols = diffCols.map{case (leftOpd, rightOpd) => leftOpd + " - " + rightOpd}
        
                (inferPrimSchema(unstructRDD,defCols), defCols)
    }
}