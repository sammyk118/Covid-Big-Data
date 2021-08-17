package trends

import session.spark.LocalSparkSession
import trends.util.DateValDiff
import covid.tables.DFTables
import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer

object sampleTrend {
  def thisisamethod: Unit = {
    val confirmed = DFTables.getCOVID_19Confirmed
    println("hooray!")
    val conDiff = DateValDiff.divideDiffDF(confirmed)
    conDiff.show()
    val cDiffNeat = conDiff.drop("Province/State", "Country/Region", "Lat", "Long")
    val cDiffNeatRDD = cDiffNeat.rdd

    val maxrdd = cDiffNeatRDD.map(row => {
      row.toSeq.fold(0)((acc, ele) =>
        if (acc.toString.toInt > ele.toString.toInt)
          acc.toString.toInt
        else
          ele.toString.toInt
      )
    })
    maxrdd.foreach(x => {
      println(x)
    })
    val countryrdd =


    //      println("Max : "+dDiffNeatRDD.fold(Row(0))( (acc,ele)=>{
    //      if (acc.toString().toInt > ele.toString().toInt){
    //        println("a thing happened")
    //        acc
    //      }
    //      else {
    //        println("another thing happened")
    //        ele
    //      }
    //    }))

    //    val dDiffNeatRDD2 = dDiffNeatRDD.map(row =>{
    //      row.toSeq.map(x => {
    //        max = x
    //      })
    //    })
    //    deaths.groupBy("Country/Region").sum("2/13/20").show
  }
}