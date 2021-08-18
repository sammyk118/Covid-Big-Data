package trends.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object DateValDiff {
    def divideDiffDF: DataFrame => DataFrame = df_handle => {
        val rc = (df_handle.rdd, df_handle.columns.toSeq)
        val diff: (Long,Long) => Long = (a,b) => a - b

        CovidRowFunctions.vicinalDateOp(diff,rc)._1
    }

    def divideDiffRDD: Tuple2[RDD[Row],Seq[String]] => Tuple2[RDD[Row],Seq[String]] = rc => {
        val (df, cols) = CovidRowFunctions.vicinalDateOp((a,b) => a - b, rc)
        (df.rdd,cols)
    }
}