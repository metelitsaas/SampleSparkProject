package ru.etl.spark.jobs.sample

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.scalatest.BeforeAndAfterEach
import org.scalatest.PrivateMethodTester
import ru.etl.spark.utils.EnvironmentConfig
import java.sql.Date


class SampleTransformsSuite
    extends QueryTest
    with TestHiveSingleton
    with BeforeAndAfterEach
    with PrivateMethodTester {

    import spark.implicits._

    // Schema of environment
    EnvironmentConfig.set("dev")

    private var st: SampleTransforms = _

    override def beforeEach(): Unit = {
        st = new SampleTransforms(spark)
    }

    test("readSourceTable transform should read DataFrame from table") {
        val sourceTable = "sourceTable"
        val sourceDf = Seq(1, 2, 3, 4, 5, 5, 5, 5, 5, 5)
            .toDF("key")
        sourceDf.createOrReplaceTempView(f"$sourceTable")

        checkAnswer(
            st.readSourceTable(f"$sourceTable"),
            spark.sql(f"SELECT * FROM $sourceTable")
        )
    }

    test("withMonthEnd transform should return valid DataFrame with month_end_report_dt column") {
        val sourceDf = Seq(
            (1, Date.valueOf("2020-01-01")),
            (2, Date.valueOf("2020-01-15")),
            (3, Date.valueOf("2020-02-12"))
        ).toDF("key", "report_dt")

        val targetDf = Seq(
            (1, Date.valueOf("2020-01-01"), Date.valueOf("2020-01-31")),
            (2, Date.valueOf("2020-01-15"), Date.valueOf("2020-01-31")),
            (3, Date.valueOf("2020-02-12"), Date.valueOf("2020-02-29"))
        ).toDF("key", "report_dt", "month_end_report_dt")

        checkAnswer(
            sourceDf.transform(st.withMonthEnd),
            targetDf
        )
    }

    test("whereReportDt transform should return valid DataFrame with where statement") {
        val sourceDf = Seq(
            (1, Date.valueOf("2020-01-01")),
            (2, Date.valueOf("2020-01-15")),
            (3, Date.valueOf("2020-02-02")),
            (3, Date.valueOf("2020-02-05")),
            (3, Date.valueOf("2020-02-15")),
            (3, Date.valueOf("2020-02-20"))
        ).toDF("key", "report_dt")

        val targetDf = Seq(
            (2, Date.valueOf("2020-01-15")),
            (3, Date.valueOf("2020-02-02")),
            (3, Date.valueOf("2020-02-05")),
            (3, Date.valueOf("2020-02-15"))
        ).toDF("key", "report_dt")

        checkAnswer(
            sourceDf.transform(st.whereReportDt("2020-01-15", "2020-02-17")),
            targetDf
        )
    }

    test("joinCar transform should join table to DataFrame") {
        val sourceDf = Seq(
            (1, "Test1"),
            (2, "Test2"),
            (3, "Test3"),
            (4, "Test4")
        ).toDF("customer_id", "customer_nm")

        val joinTable = "joinTable"
        val joinDf = Seq(
            (1, "Car1"),
            (2, "Car2"),
            (4, "Car4"),
            (4, "Car5")
        ).toDF("customer_id", "car_nm")
        joinDf.createOrReplaceTempView("joinTable")

        val targetDf = Seq(
            (1, "Test1", "Car1"),
            (2, "Test2", "Car2"),
            (3, "Test3", null),
            (4, "Test4", "Car4"),
            (4, "Test4", "Car5")
        ).toDF("customer_id", "customer_nm", "car_nm")

        checkAnswer(
            sourceDf.transform(st.joinCar(f"$joinTable")),
            targetDf
        )
    }

    test("load transform should write DataFrame to table") {
        val targetTable = "targetTable"
        val sourceDf = Seq(1, 2, 3, 4, 5, 5, 5, 5, 5, 5)
            .toDF("key")

        spark.sql(f"CREATE TABLE $targetTable (key int)")

        sourceDf.transform(st.load(f"$targetTable"))

        checkAnswer(
            spark.sql(f"SELECT * FROM $targetTable"),
            sourceDf
        )
    }

}