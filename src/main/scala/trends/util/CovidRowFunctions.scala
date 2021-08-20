package trends.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object CovidRowFunctions {
    private val dateMatchRegEx = "\\d".r
    //private val dateMatchRegEx = "\\d{1,4}?\\/\\d{1,2}?\\/\\d".r
    //private val dateMatchRegEx = "(\\d{1,4}?\\/\\d{1,2}?)\\w".r

    def aggregateDateOp[R](base_case: R)(tOp: Row => (R, String) => R, rc: Tuple2[RDD[Row],Seq[String]]): RDD[Row] = {
        val guard: Row => (R, String) => Boolean = row => {case (f, v) => isDateCol(v)}
        val fOp: Row => (R, String) => R = row => {case (f, v) => f}

        val fofl = RowFunctionBase.withGuard(guard, tOp, fOp)

        RowFunctionBase.rowFoldRDD(base_case)(rc,fofl)
    }

    def vicinalDateOp(op: (Long,Long) => Long, rc: Tuple2[RDD[Row],Seq[String]], newCols: Seq[Tuple2[String,String]] => Seq[String] = defVicinalDateColsOp _): (DataFrame,Seq[String]) = {
        val rTc = (rc._1, colSepJoinZip(rc._2))

        val guard: Row => (Seq[Any],Tuple2[String,String]) => Boolean =
            row => {case (f,(lftOpd,rtOpd)) => lftOpd == rtOpd} 

        val tOp: Row => (Seq[Any],Tuple2[String,String]) => Seq[Any] =
            row => {case (f, (lftOpd, rtOpd)) => f ++ Seq[Any](row.getAs[Any](lftOpd))}

        val fOp: Row => (Seq[Any],Tuple2[String,String]) => Seq[Any] =
            row => {
                case (f, (lftOpd, rtOpd)) => 
                    f ++ Seq[Any](
                        op(row.getAs[Any](lftOpd).toString.toLong, row.getAs[Any](rtOpd).toString.toLong)
                    )
            }
        
        val fofl = RowFunctionBase.withGuard(guard, tOp, fOp)
        
        RowFunctionBase.biColFuncDF(rTc,fofl,newCols)
    }

    def isDateCol(columnName: String): Boolean = {
        dateMatchRegEx.findFirstIn(columnName).isDefined
    }

    def colSepJoinZip(columns: Seq[String]): Seq[Tuple2[String,String]] = {
        val dateCols  = columns.filter(col => isDateCol(col))
        val otherCols = columns.filter(col => !isDateCol(col))
        val priOperandDate = dateCols.drop(1)
        val secOperandDate = dateCols.dropRight(1)

        (otherCols ++ priOperandDate).zip(otherCols ++ secOperandDate)
    }

    private def defVicinalDateColsOp(cols: Seq[Tuple2[String,String]]): Seq[String] = {
        cols.map{case (lftOpd,_) => lftOpd}
    }
}