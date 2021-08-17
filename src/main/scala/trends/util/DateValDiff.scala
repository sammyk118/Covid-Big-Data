package trends.util

import session.spark.LocalSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType,StringType, StructField, StructType}

object DateValDiff {
    def divideDiffDF: DataFrame => DataFrame = df_handle => {
        rowToRowJoin(divideDiff(df_handle.rdd -> df_handle.columns))
    }

    def divideDiffRDD: Tuple2[RDD[Row],Seq[String]] => Tuple2[RDD[Row],Seq[String]] = {
        case (rdd_handle, columns) =>
            val joinedTable = rowToRowJoin(divideDiff(rdd_handle, columns))
            (joinedTable.rdd, joinedTable.columns)
    }

    def rowToRowJoin: Tuple2[(DataFrame,Seq[String]),(DataFrame, Seq[String])] => DataFrame = {
        case ((othCols, othNCN),(valCols,newCN)) =>
            val joinedCols = othNCN ++ newCN
            val joinedTable = othCols.rdd.zip(valCols.rdd)
                                .map{case (leftT, rightT) => Row((leftT.toSeq ++ rightT.toSeq):_*)}
            
            inferPrimSchema(joinedTable -> joinedCols)
    }

    def divideDiff: Tuple2[RDD[Row],Seq[String]] => Tuple2[(DataFrame,Seq[String]),(DataFrame, Seq[String])] = {
        case (rdd_handle, columns) => 
            val dateCols  = columns.filter(col => "\\d".r.findFirstIn(col).isDefined)
            val otherCols = columns.filter(col => "\\d".r.findFirstIn(col).isEmpty)
            val priOperandDate = dateCols.drop(1)
            val secOperandDate = dateCols.dropRight(1)

            (createDataFrame(rdd_handle -> otherCols), diffRDD(rdd_handle -> priOperandDate.zip(secOperandDate)))
    }

    def createRDD: Tuple2[RDD[Row], Seq[String]] => RDD[Row] = rc => createDataFrame(rc)._1.rdd

    def createDataFrame: Tuple2[RDD[Row],Seq[String]] => (DataFrame,Seq[String]) = {
        case (rdd_handle, columns) =>
            val unstructRDD = rdd_handle.map(row => Row(columns
                .foldLeft(Seq[Any]())((f, v) => f ++ Seq[Any](row.getAs[Any](v))):_*))
            
            (inferPrimSchema(unstructRDD -> columns), columns)
    }

    def inferPrimSchema: Tuple2[RDD[Row],Seq[String]] => DataFrame = {
        case (rdd_handle, columns) =>
            val keyVal = columns.zip(rdd_handle.take(1)(0).toSeq)
            val structType = StructType(keyVal.map{case (col,value) => StructField(col,inferType(value),true)})

            LocalSparkSession().createDataFrame(rdd_handle,structType)
    }

    def inferType: Any => DataType = {
        case null => StringType
        case n if(n.isInstanceOf[Long]) => LongType
        case n if(n.isInstanceOf[Double]) => DoubleType
        case s => StringType
    }

    def diffRDD: Tuple2[RDD[Row],Seq[(String,String)]] => (DataFrame, Seq[String]) = {
        case (rdd_handle, diffCols) =>
            val unstructRDD = rdd_handle.map(row => Row(diffCols
                .foldLeft(Seq[Int]())((f, v) => f ++ Seq[Int](row.getAs[Int](v._1) - row.getAs[Int](v._2))):_*))
            

            val defCols = diffCols.map{case (leftOpd, rightOpd) => leftOpd}

        
                (inferPrimSchema(unstructRDD,defCols), defCols)
    }
}