import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine
import session.spark.LocalSparkSession
import covid.tables.DFTables
import trends.{PopulationDensity, sampleTrend, TrendTwo} //enter your query imports here

object Start {
  val spark = LocalSparkSession()

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    spark.sparkContext.setLogLevel("WARN") //reduces terminal clutter
    println("created spark session")

    def input() {
      println("What would you like to do? \nGTSampleTrend: 0 \nT1PopulationDensity: 1 \nT2TrendName: 2 \nT3TrendName: 3 \nT4TrendName: 4\n")
      print("enter your command here: ")
      val command = readLine()
      if (command == "0") {
        sampleTrend.thisisamethod
      }
      if (command == "1") {
        PopulationDensity.deflateDFTable
      }
      if (command == "2") {
        TrendTwo.first
        println("this command still needs to be set up")
      }
      if (command == "3") {
        println("this command still needs to be set up")
      }
      if (command == "4") {
        println("this command still needs to be set up")
      }
      exit()
    }

    def exit() {
      System.exit(0)
    }
    input()
  }
}
