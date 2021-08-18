package trends.util

import session.spark.LocalSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType, StructField, StructType}

object RowFunctionBase {
    def rowFoldRDD[R](base_case: R)(rc: Tuple2[RDD[Row],Seq[String]], fofl: Row => (R, String) => R): RDD[Row] = {
        createRDD(base_case)(fofl,rc)
    }

    /*
     * biColFuncRDD might never be called. It will be left here for the time being in case
     * some, as of yet not forseen, utility can be gained from it.
     */
    def biColFuncRDD(rc: Tuple2[RDD[Row],Seq[Tuple2[String,String]]], fofl: Row => (Seq[Any],Tuple2[String,String]) => Seq[Any]): RDD[Row] = {
        createRDD(Seq[Any]())(fofl,rc)
    }

    def withGuard[T,R](guard: Row => (R, T) => Boolean,trueFofl: Row => (R, T) => R, falseFofl: Row => (R, T) => R): Row => (R, T) => R = {
        (row: Row) => (f: R, v: T) => if(guard(row)(f,v)) trueFofl(row)(f,v) else falseFofl(row)(f,v)
    }

    def createRDD[T,R](base_case: R)(fofl: Row => (R, T) => R, rc: Tuple2[RDD[Row],Seq[T]]): RDD[Row] = rc match {
        case (rdd_handle, columns) =>
            val rowColFunction = (col: Seq[T]) => (row: Row) => col.foldLeft(base_case)(fofl(row))
            _createRDD(rc, rowColFunction)
    }

    def _createRDD[T,R](rc: Tuple2[RDD[Row], Seq[T]], foColR: Seq[T] => Row => R): RDD[Row] = rc match {
        case (rdd_handle, columns) =>
            rdd_handle.map(row => {
                val res: R = foColR(columns)(row)
                if(res.isInstanceOf[Seq[Any]])
                    Row(res.asInstanceOf[Seq[Any]]:_*)
                else
                    Row(res)
            })
    }

    /*
     * The rowFoldDF[R] function may not work depending on the base case because it will automatically
     * make a call to inferPrimSchema which will only work on primitive types and Strings.
     */
    // def rowFoldDF[R](base_case: R)(rc: Tuple2[RDD[Row],Seq[String]], fofl: Row => (R, String) => R, newCols: Seq[String] => Seq[String]): (DataFrame,Seq[String]) = {
    //     _createDataFrame(base_case)(fofl,rc,newCols)
    // }

    def biColFuncDF(rc: Tuple2[RDD[Row],Seq[Tuple2[String,String]]], fofl: Row => (Seq[Any],Tuple2[String,String]) => Seq[Any], newCols: Seq[Tuple2[String,String]] => Seq[String]): (DataFrame,Seq[String]) = {
        _createDataFrame(Seq[Any]())(fofl,rc,newCols)
    }

    def createDataFrame: Tuple2[RDD[Row],Seq[String]] => (DataFrame,Seq[String]) = rc => {
        val rowColFunction: Row => (Seq[Any],String) => Seq[Any] = row => (f,v) => f ++ Seq[Any](row.getAs[Any](v))
        
        _createDataFrame(Seq[Any]())(rowColFunction,rc)
    }

    def _createDataFrame[T,R](base_case: R)(fofl: Row => (R, T) => R, rc: Tuple2[RDD[Row],Seq[T]], newCols: Seq[T] => Seq[String] = defColsOp _): (DataFrame,Seq[String]) = {
        val unstructRDD = createRDD(base_case)(fofl, rc)
        val newColumns = newCols(rc._2)

        (inferPrimSchema(unstructRDD -> newColumns), newColumns)
    }

    private def defColsOp[T](cols: Seq[T]): Seq[String] = {
        if (cols.head.isInstanceOf[String])
            cols.asInstanceOf[Seq[String]]
        else
            cols.map(_.toString)
    }

    def inferPrimSchema: Tuple2[RDD[Row],Seq[String]] => DataFrame = {
        case (rdd_handle, columns) =>
            val keyVal = columns.zip(rdd_handle.take(1)(0).toSeq)
            val structType = StructType(keyVal.map{case (col,value) => StructField(col,inferType(value),true)})

            LocalSparkSession().createDataFrame(rdd_handle,structType)
    }

    def inferType: Any => DataType = {
        case null                           => StringType
        case n if(n.isInstanceOf[Long])     => LongType
        case n if(n.isInstanceOf[Double])   => DoubleType
        case _                              => StringType
    }
}