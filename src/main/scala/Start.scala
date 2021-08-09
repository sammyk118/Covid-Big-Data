import org.apache.spark.sql.SparkSession

object Start {
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    spark.sparkContext.setLogLevel("WARN") //reduces terminal clutter
    println("created spark session")

    println("Hello, world! this is a new line")

  }
}
