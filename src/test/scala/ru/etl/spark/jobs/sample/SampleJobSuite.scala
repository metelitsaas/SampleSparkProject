package ru.etl.spark.jobs.sample

import org.scalatest.FunSuite

class SampleJobSuite extends FunSuite {

    // Init job
    private val args = Array("ENV=dev", "reportBeginDate=2020-02-20", "reportEndDate=2020-02-25")
    SampleJob.main(args)

    ignore("SampleJob should stop SparkSession in the end") {}

}
