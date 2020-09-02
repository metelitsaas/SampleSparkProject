package ru.etl.spark.functions

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import org.scalatest.BeforeAndAfterEach


class TransformsSuite
    extends QueryTest
        with SharedSparkSession
        with BeforeAndAfterEach {

    import testImplicits._

    private var t: Transforms = _

    override def beforeEach(): Unit = {
         t = new Transforms(spark)
    }

    test("withProcessedDttm transform should return valid DataFrame") {
        val sourceDf = Seq(1, 2, 3)
            .toDF("int")
            .transform(t.withProcessedDttm)

        val targetDf =  Seq(
            (1, Timestamp.valueOf(f"${Functions.processedDttm}")),
            (2, Timestamp.valueOf(f"${Functions.processedDttm}")),
            (3, Timestamp.valueOf(f"${Functions.processedDttm}"))
        ).toDF("int", "processed_dttm")

        checkAnswer(
            sourceDf,
            targetDf
        )
    }

    test("withProcessedDttm transform should return valid type of column") {
        val sourceDf = Seq(1, 2, 3)
            .toDF("int")
            .transform(t.withProcessedDttm)

        val columnType = sourceDf
            .schema
            .fields
            .filter(field => field.name == "processed_dttm")
            .map(field => field.dataType)
            .head

        assert(columnType == TimestampType)
    }

    test("broadcastFrequentSkew transform should return same DataFrame") {
        val sourceDf = Seq(1, 2, 3, 4, 5, 5, 5, 5, 5, 5)
            .toDF("int")

        checkAnswer(
            sourceDf,
            sourceDf.transform(t.broadcastFrequentSkew(Seq(col("int"))))
        )
    }

    // TODO: Add broadcastFrequentSkew test
    ignore("broadcastFrequentSkew transform should broadcast skewed part of DataFrame") {}

    test("""castTimestampsToStrings transform should return DataFrame with TimeStamp columns
            casted to String columns""") {
        val sourceDf = Seq((1, Timestamp.valueOf("2020-01-01 12:00:00")))
            .toDF("int", "datetime")

        val sourceColumnType = sourceDf
            .schema
            .fields
            .filter(field => field.name == "datetime")
            .map(field => field.dataType)
            .head

        val targetColumnType = sourceDf
            .transform(t.castTimestampsToStrings)
            .schema
            .fields
            .filter(field => field.name == "datetime")
            .map(field => field.dataType)
            .head

        assert(sourceColumnType == TimestampType && targetColumnType == StringType)
    }

    test("repartitionByType transform should return repartitioned DataFrame") {
        val sourceDf = Seq(1, 2, 3, 4, 5, 5, 5, 5, 5, 5)
            .toDF("int")

        val target1Df = sourceDf
            .transform(t.repartitionByType("plan"))

//        val target2Df = sourceDf
//            .transform(t.repartitionByType())

        assert(sourceDf.rdd.getNumPartitions >= target1Df.rdd.getNumPartitions)
//        assert(sourceDf.rdd.getNumPartitions <= target2Df.rdd.getNumPartitions)
    }

}
