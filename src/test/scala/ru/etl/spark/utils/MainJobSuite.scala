package ru.etl.spark.utils

import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession


class MainJobSuite extends FunSuite {

    object ObjectMainJobTest extends MainJob {

        var sparkTest: SparkSession = _

        override def run(sparkSessionConfig: SparkSessionConfig): Unit = {
            sparkTest = sparkSessionConfig.spark
        }
    }

    test("MainJob should create SparkSession") {
        val args = Array("ENV=dev")
        ObjectMainJobTest.main(args)

        val spark = ObjectMainJobTest.sparkTest

        assert(spark.sql("select count(*)").first.get(0) == 1)

    }

    test("MainJob should define application name") {
        val args = Array("ENV=dev")
        ObjectMainJobTest.main(args)

        val spark = ObjectMainJobTest.sparkTest

        val appName = spark.conf.get("spark.app.name")

        assert(appName == "ObjectMainJobTest")
    }

}
