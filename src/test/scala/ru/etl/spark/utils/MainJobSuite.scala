package ru.etl.spark.utils

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.SparkSession


class MainJobSuite extends FunSuite with BeforeAndAfterAll {

    private var spark: SparkSession = _

    object ObjectMainJobTest extends MainJob {

        var sparkTest: SparkSession = _

        override def run(sparkSessionConfig: SparkSessionConfig): Unit = {
            sparkTest = sparkSessionConfig.spark
        }
    }

    override def beforeAll(): Unit = {
        super.beforeAll()

        val args = Array("ENV=dev")
        ObjectMainJobTest.main(args)
        spark = ObjectMainJobTest.sparkTest
    }

    override def afterAll(): Unit = {
        try {
            spark.stop()
        } finally {
            super.afterAll()
        }
    }

    test("MainJob should create SparkSession") {

        assert(spark.sql("select count(*)").first.get(0) == 1)

    }

    test("MainJob should define application name") {
        val appName = spark.conf.get("spark.app.name")

        assert(appName == "ObjectMainJobTest")
    }
}
