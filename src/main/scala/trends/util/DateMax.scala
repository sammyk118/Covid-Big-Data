package trends.util

import session.spark.LocalSparkSession

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.DataFrameReader

object DateMax {
    def findMaxIncludeCountryAndDate(df: DataFrame, newColNames: Seq[String] = Seq[String]( "Country/Region","Date","Max")): DataFrame = {
        val rc: Tuple2[RDD[Row],Seq[String]] = (df.rdd, df.columns)

        val tOp: Row => (Seq[Any],String) => Seq[Any] = row => {
            case (coDaMax, col) =>
                if(coDaMax(2).toString.toLong > row.getAs[Any](col).toString.toLong)
                    coDaMax
                else
                    Seq[Any](row.getAs[String]("Country/Region"), col, row.getAs[Any](col).toString.toLong)
        }

        RowFunctionBase.inferPrimSchema(CovidRowFunctions.aggregateDateOp(Seq[Any]( "", "", 0l))(tOp, rc) -> newColNames)
    }
}