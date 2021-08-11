package trends
import session.spark.LocalSparkSession

import covid.tables.DFTables
import org.apache.spark.sql.{DataFrame, SparkSession}

object sampleTrend{

  def thisisamethod: Unit = {
    DFTables.getCOVID_19Deaths.select("*").show()
    println("hooray!")
  }
}