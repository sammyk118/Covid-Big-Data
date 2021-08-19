package trends

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.col
import session.spark.LocalSparkSession

import covid.tables.DFTables
import trends.util.DateValDiff

import scala.collection.immutable.Range.Int

object TrendTwo {


  val listofCountry = "'Afghanistan', 'Bhutan', 'Bangladesh','Nepal', 'India', 'Maldives', 'Pakistan', 'Sri Lanka'"

  def createFiltered_country(mytable: DataFrame): DataFrame = {
    mytable.createOrReplaceTempView("temp")
    val spark = LocalSparkSession()
    val selected_column = createSum_select(mytable.columns, "temp")
    spark.sql(s"select $selected_column from temp where `Country/Region` in ($listofCountry) group by `Country/Region` ")
    //spark.sql(s"select * from temp where `Country/Region` in ($listofCountry) ")

  }

  def createSum_select(columnName: Array[String], viewName: String): String = {
    val (ltcolumn, rtcolumn) = sparate_header_dates(columnName)
    val map_rtcolumn = rtcolumn.map(coll => s"sum(`$coll`)")
    val connect = format_column_explicit(ltcolumn.drop(1).dropRight(2), viewName) ++ map_rtcolumn
    val formatted_string = connect.mkString(", ")
    formatted_string
  }


  def first: Unit = {
    val spark = LocalSparkSession()
    val mytable1 = DFTables.getCOVID_19Confirmed
    val mytable2 = DFTables.getCOVID_19Recovered
    val mytable3 = DFTables.getCOVID_19Deaths
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")



//     val date_table1 = DateValDiff.divideDiffDF(createFiltered_country(mytable1))
//     val date_table2 = DateValDiff.divideDiffDF(createFiltered_country(mytable2))
//    val date_table3 = DateValDiff.divideDiffDF(createFiltered_country(mytable3))
//
//          val (lfCols, rtCols) = sparate_header_dates(date_table1.columns)
//          val formatted_left_column = format_column_explicit(lfCols, "infection1")
//          val rtcol_infection = format_column_explicit(rtCols, "infection1")
//          val rtcol_recover = format_column_explicit(rtCols, "recover1")
//          val rtcol_death = format_column_explicit(rtCols, "death1")
//          val composite_rtcol = rtcol_infection.zip(rtcol_recover.zip(rtcol_death)).map { case (infection, (recover, death)) => s"$infection - $recover - $death" }
//          val combined_col = formatted_left_column ++ composite_rtcol
//          val selected_col = combined_col.mkString(", ")
//
//          date_table1.createOrReplaceTempView("infection1")
//         date_table2.createOrReplaceTempView("recover1")
//         date_table3.createOrReplaceTempView("death1")
//
//    spark.sql(s" create table if not exists final_table1 as ( select $selected_col from infection1 inner join recover1 on infection1.`Country/Region` = recover1.`Country/Region` inner join death1 on recover1.`Country/Region` = death1.`Country/Region`) ")

    spark.sql("select * from final_table1").show()

  }

  def format_column_explicit(column: Array[String], view_name: String): Array[String] = {
    column.map(x => s"$view_name.`$x`")
  }
// Separate date and left columns(country/region, provinces, Lat, Long) as right columns and left columns
  def sparate_header_dates(column: Array[String]): (Array[String], Array[String]) = {
    val leftCols = column.filter(col => "\\D".r.findFirstIn(col).isDefined)
    val rightCols = column.filter(col => "\\d".r.findFirstIn(col).isDefined)
    (leftCols, rightCols)

  }
}
