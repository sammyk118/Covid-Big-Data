package trends.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object DateMax {
    def findMaxIncludeCountryAndDate(df: DataFrame): RDD[Row] = {
        val rc: Tuple2[RDD[Row],Seq[String]] = (df.rdd, df.columns)

        val tOp: Row => (Tuple3[String,String,Long],String) => Tuple3[String,String,Long] = row => {
            case ((country, date, value), col) =>
                if(value > row.getAs[Any](col).toString.toLong)
                    (country, date, value)
                else
                    (row.getAs[String]("Country/Region"), col, row.getAs[Any](col).toString.toLong)
        }

        CovidRowFunctions.aggregateDateOp(("", "", 0l))(tOp, rc)
    }
}