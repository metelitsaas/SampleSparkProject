package ru.etl.spark.utils

import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import java.util.TimeZone

/* SparkSession jobs configuration class
@param appName: Name of application */
class SparkSessionConfig(appName: String, master: String) {
    // Default configs
    private val SPARK_TIMEZONE = "GMT"
    private val JAVA_TIMEZONE = "GMT+03:00"

    // Set SparkSession
    val spark: SparkSession = SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", SPARK_TIMEZONE)
        .enableHiveSupport
        .getOrCreate()

    // Set Logger
    val logger: Logger = Logger.getLogger(appName)
    logger.setLevel(Level.INFO)

    // Java timezone
    TimeZone.setDefault(TimeZone.getTimeZone(JAVA_TIMEZONE))

}