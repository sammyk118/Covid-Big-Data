package trends
import trends.util.DateValDiff
import covid.tables.DFTables
import trends.util.DateMax
import session.spark.LocalSparkSession
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object T0worstDays {
  def findMax: Unit = {
    val spark      = LocalSparkSession()
    import spark.implicits._


    val confirmed = DFTables.getCOVID_19Confirmed
    println("hooray!")
    val filtered = confirmed.filter(
    val conDiff = DateValDiff.divideDiffDF(confirmed)
    conDiff.cache()
    conDiff.show()
    val cDiffNeat = conDiff.drop("Province/State", "Country/Region", "Lat", "Long")
    val cDiffNeatRDD = cDiffNeat.rdd
    var count: Int = 0
    val maxrdd = cDiffNeatRDD.map(row => {
      count += 1
      row.toSeq.fold(0)((acc, ele) =>
        {acc.toString.toInt max ele.toString.toInt}
//          acc.toString.toInt
//        else
//          ele.toString.toInt
      )
    })
    val maxDays = DateMax.findMaxIncludeCountryAndDate(conDiff)
    maxDays.foreach(x => println(x))
//    val maxDaysWithSchema: DataFrame = spark.createDataFrame(maxDays).toDF("country", "Date", "num")
//    val zipmaxRDD = maxrdd.zipWithIndex
//    zipmaxRDD.zip(x => {
//      println(x)
//    })
    //val country rdd =
    //need tp get country name and date alongside max val. would getting the vertical difference help find the corresponding date and country for max val in covid_19_data?
    //can i just get column name in the map?

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