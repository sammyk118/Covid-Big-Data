package trends.util

import session.spark.LocalSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StringType, StructField, StructType}

object RowFunctionBase {

    /*
     *  A base, yet generic template for formulating folds on an RDD's header and consequently on each RDD row; it is
     *  intended for formulating aggregate functions, but it is likely capable of more.
     *  
     *  @param base_case The base case for the fold. This can vary in type since the fold can be used for aggregation.
     *  @param rc A tuple containing an RDD handle and a sequence of column names
     *  @param fofl A function of foldLeft which I have fondly come to call fawful. It is a curried function that foldLeft
     *          will execute; however, the function takes a parameter Row and is curried with with the foldLeft function;
     *          this is so that the foldLeft function will have the Row as a closure and be able to give the effect of
     *          folding the Row. Ideally, this fofl should be created with the 'withGuard' function.
     */
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

    /*
     *  A wrapper function that creates a fofl (fawful) by aggregating a predicate, and two fofl (fawful) functions. Please see
     *  the fofl (fawful) parameter comment on rowFoldRDD for an explanation of fofl (fawful). This function was designed with
     *  the DateValDiff object in mind; there was a need to perform one function on non-date columns and a different function
     *  on date columns; so, this aggregates the three functions so that fofl created by this function becomes shorter to express
     *  when writing either a more concrete rowFoldRDD/rowFoldDF or a more concrete biColFuncRDD/biColFuncDF function.
     * 
     *  @param guard A predicate function that, if true, will cause trueFofl to be applied and, when false, when false causes falseFofl
     *  @param trueFofl fofl to execute when predicate is true
     *  @param falseFofl fofl to execute when predicate is false
     */
    def withGuard[T,R](guard: Row => (R, T) => Boolean,trueFofl: Row => (R, T) => R, falseFofl: Row => (R, T) => R): Row => (R, T) => R = {
        (row: Row) => (f: R, v: T) => if(guard(row)(f,v)) trueFofl(row)(f,v) else falseFofl(row)(f,v)
    }

    /*
     *  A generic function for transforming a structured RDD to an unstructured RDD. This version of createRDD only
     *  defines the general structure of the fold function that is passed on to the more generic _createRDD
     * 
     *  @param base_case The base case for the fold; it is defined as type R to handle variable cases
     *  @param fofl The definition of fofl; the structure of this curried function was determined by the necessity
     *              of passing a Row handle as a closure
     *  @param rc A tuple containing (Rdd handle, Column names sequence)
     */
    def createRDD[T,R](base_case: R)(fofl: Row => (R, T) => R, rc: Tuple2[RDD[Row],Seq[T]]): RDD[Row] = rc match {
        case (rdd_handle, columns) =>
            val rowColFunction = (col: Seq[T]) => (row: Row) => col.foldLeft(base_case)(fofl(row))
            _createRDD[T,R](rc, rowColFunction)
    }

    /*
     *  The generic definition for transforming a structured RDD to an unstructured RDD. This function is currently
     *  incomplete as it can only return an RDD of type Row. If possible, it would be nice to implement it in such
     *  a way that it could return an RDD of variable type. Additionally, it is currently dependent on the column names 
     *  and structure of the RDD that are passed to it. If that could also be abstracted out, it would make it more
     *  adaptable to other use cases.
     */
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

    /*
     *  The counterpart to biColFuncRDD. It simply calls _createDataFrame which will call biColFuncRDD and create a DF from the returned RDD
     */
    def biColFuncDF(rc: Tuple2[RDD[Row],Seq[Tuple2[String,String]]], fofl: Row => (Seq[Any],Tuple2[String,String]) => Seq[Any], newCols: Seq[Tuple2[String,String]] => Seq[String]): (DataFrame,Seq[String]) = {
        _createDataFrame(Seq[Any]())(fofl,rc,newCols)
    }

    /*
     *  Currently not being used. It is a somewhat generic form of the original, concrete createDataFrame function in DateValDiff.
     *  Since creating the more general functions, this function has become obsolete
     */
    def createDataFrame: Tuple2[RDD[Row],Seq[String]] => (DataFrame,Seq[String]) = rc => {
        val rowColFunction: Row => (Seq[Any],String) => Seq[Any] = row => (f,v) => f ++ Seq[Any](row.getAs[Any](v))
        
        _createDataFrame(Seq[Any]())(rowColFunction,rc)
    }

    /*
     *  Generic form of createDataFrame which just wraps around createRDD. It was only made with generic types to be able to pass
     *  them on to the partially generalized createRDD, _createRDD functions. It also allows for passing in a function, newCols to
     *  define how to name all new columns; however, it is optional since it also provides a default function for naming the new columns;
     *  Finally, createDataFrame calls inferPrimSchema to infer the schema of the new rdd to then return a DataFrame-Columns tuple.
     */
    def _createDataFrame[T,R](base_case: R)(fofl: Row => (R, T) => R, rc: Tuple2[RDD[Row],Seq[T]], newCols: Seq[T] => Seq[String] = defColsOp _): (DataFrame,Seq[String]) = {
        val unstructRDD = createRDD(base_case)(fofl, rc)
        val newColumns = newCols(rc._2)

        (inferPrimSchema(unstructRDD -> newColumns), newColumns)
    }

    /*
     *  The private, default column naming function for _createDataFrame
     */
    def defColsOp[T](cols: Seq[T]): Seq[String] = {
        if (cols.head.isInstanceOf[String])
            cols.asInstanceOf[Seq[String]]
        else
            cols.map(_.toString)
    }

    /*
     *  This function collects the first Row from the RDD handle and pairs each Row value with each column name.
     *  The value, of the (column-name, row-value) pair, is then matched with predetermined primitive types.
     *  That information is used to create a StructField, and the Sequence of StructFields are then used to
     *  create the StructType which is ultimately used to "give structure" to the RDD and create a DataFrame.
     */
    def inferPrimSchema: Tuple2[RDD[Row],Seq[String]] => DataFrame = {
        case (rdd_handle, columns) =>
            val keyVal = columns.zip(rdd_handle.take(1)(0).toSeq)
            val structType = StructType(keyVal.map{case (col,value) => StructField(col,inferType(value),true)})

            LocalSparkSession().createDataFrame(rdd_handle,structType)
    }

    /*
     *  This function matches a row value with one of three predetermined primitive types:
     *  String, Long, or Double
     *  It, then, returns the corresponding DataType (from org.apache.spark.sql.{DataType, StringType, ...})
     */
    def inferType: Any => DataType = {
        case null                           => StringType
        case n if(n.isInstanceOf[Long])     => LongType
        case n if(n.isInstanceOf[Double])   => DoubleType
        case _                              => StringType
    }
}