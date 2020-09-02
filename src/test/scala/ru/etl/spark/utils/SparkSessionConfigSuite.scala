package ru.etl.spark.utils

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import java.util.TimeZone


class SparkSessionConfigSuite extends FunSuite with BeforeAndAfterAll {

    private var spark: SparkSession = _
    private var logger: Logger = _

    override def beforeAll(): Unit = {
        super.beforeAll()

        val master = "local"
        val sparkSessionConfig = new SparkSessionConfig("SparkSessionConfigTest", master)
        spark = sparkSessionConfig.spark
        logger = sparkSessionConfig.logger
    }

    override def afterAll(): Unit = {
        try {
            spark.stop()
        } finally {
            super.afterAll()
        }
    }

    test("SparkSession should initialize spark") {
        val tmpValue = spark
            .sql("select count(*)")
            .first
            .get(0)

        assert(tmpValue == 1)
    }

    test("SparkSession configs should set properly") {
        val appName = spark.conf.get("spark.app.name")
        val dynamicPartition = spark.conf.get("hive.exec.dynamic.partition.mode")
        val partitionOverwrite = spark.conf.get("spark.sql.sources.partitionOverwriteMode")
        val sparkTimezone = spark.conf.get("spark.sql.session.timeZone")
        val javaTimezone = TimeZone.getDefault.getDisplayName
        val hiveSupport = spark.conf.get("spark.sql.catalogImplementation")

        assert(appName == "SparkSessionConfigTest")
        assert(dynamicPartition == "nonstrict")
        assert(partitionOverwrite == "dynamic")
        assert(sparkTimezone == "GMT")
        assert(javaTimezone == "GMT+03:00")
        assert(hiveSupport == "hive")
    }

    test("logger should print message") {
        logger.info("Logger info message")
        logger.warn("Logger warn message")
    }

}
