package trends
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import session.spark.LocalSparkSession
import covid.tables.DFTables
import org.apache.spark.sql.{DataFrame, SparkSession}
object TrendTwo {


  def first: Unit = {
    val spark = LocalSparkSession()
    val mytable = DFTables.getCOVID_19Deaths
    val sc = spark.sparkContext
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val rt = sc.textFile("input/time_series_covid_19_recovered.csv")
    rt.collect.foreach(println)
  }
}