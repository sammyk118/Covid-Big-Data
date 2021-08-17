package session.spark

import org.apache.spark.sql.SparkSession

object LocalSparkSession {
    private var lss: SparkSession   = null

    private val app_name            = "hello hive"
    private val spark_master_set    = "spark.master"
    private val spark_master        = "local"
    private val hadoop_home_set     = "hadoop.home.dir"
    private val hadoop_home         = "C:\\winutils"

    def apply(): SparkSession = if (lss == null) {lss = createSession; lss} else lss

    private def createSession: SparkSession = {
        System.setProperty(hadoop_home_set, hadoop_home)

        SparkSession
        .builder
        .appName(app_name)
        .config(spark_master_set, spark_master)
        .getOrCreate()
    }
}