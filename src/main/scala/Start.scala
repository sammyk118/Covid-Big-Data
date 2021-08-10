import org.apache.spark.sql.SparkSession

import session.spark.LocalSparkSession
import covid.tables.DFTables

object Start {
  val spark = LocalSparkSession()

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    spark.sparkContext.setLogLevel("WARN") //reduces terminal clutter
    println("created spark session")

    println("Hello, world! this is a new line")

    //Testing table creation -- asserting that the headers were read as column names
    DFTables.getCOVID_19Recovered.select("Province/State").show()

    System.exit(0)
  }
}
