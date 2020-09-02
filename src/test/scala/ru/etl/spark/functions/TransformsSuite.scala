package ru.etl.spark.functions

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.TimestampType
import java.sql.Timestamp


class TransformsSuite extends QueryTest with SharedSparkSession {

    import testImplicits._

    val t = new Transforms(spark)

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

    test("broadcastFrequentSkew transform should return valid DataFrame") {

    }

}
