package trends

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import session.spark.LocalSparkSession
import covid.tables.DFTables

import scala.collection.immutable.Range.Int

object TrendTwo {


  def first: Unit = {
    // val mysession = SparkSession.builder.master("local").appName("scala").enableHiveSupport().getOrCreate()
    val spark = LocalSparkSession()
    val mytable = DFTables.getCOVID_19Recovered
    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    mytable.createOrReplaceTempView("recover")
    //spark.sql ("select * from recover").show(500, 300, false)
    spark.sql("select * from recover where Lat = 40.0")
    val old_format = spark.sql("select * from recover where Lat = 40.0 ")
    val dropColumn = old_format.drop("Province/State", "Country/Region", "Lat", "Long")
    val rdd1 = dropColumn.rdd
    //rdd1.collect.foreach(println)
    // val rdd2 = rdd1.map(x => { var row = Seq(x);println(row)})
    //val rdd2 = rdd1.map(x => x.toSeq.map(y => x(30).toString.toInt - x(29).toString.toInt))
    val rdd2 = rdd1.map(x => x.toSeq.zipWithIndex.map { case (y, idx) => if (idx < x.length - 1 && x(idx + 1).toString.toInt - x(idx).toString.toInt >= 0) {
      x(idx + 1).toString.toInt - x(idx).toString.toInt
    }
    else if (idx < x.length - 1) {
      0
    }
    else {
      0
    }})
    rdd2.collect.foreach(println)


    //rdd2.collect.foreach(println)

    //new_format.collect.foreach(println)

    //val revover2 = rdd1.filter.Int(x => (x(1)+x(0)))


  }
}