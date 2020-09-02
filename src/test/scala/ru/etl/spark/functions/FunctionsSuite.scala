package ru.etl.spark.functions

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions._
import org.scalatest.FunSuite


class FunctionsSuite extends FunSuite {

    test("processedDttm should return valid value") {
        val format = "yyyy-MM-dd HH:mm:ss"
        val javaDttm = LocalDateTime.now().format(DateTimeFormatter.ofPattern(format))
        val processedStaticDttm = Functions.processedDttm

        assert(javaDttm == processedStaticDttm)

    }

    test("stringToColumns should return valid value") {
        val str1 = "column1, column2, column3"
        val str2 = "column1, column2"
        val str3 = "column1"

        assert(Functions.stringToColumns(str1) == Seq(
            col("column1"),
            col("column2"),
            col("column3")))
        assert(Functions.stringToColumns(str2) == Seq(
            col("column1"),
            col("column2")))
        assert(Functions.stringToColumns(str3) == Seq(col("column1")))

    }

    test("columnsToString should return valid value") {
        val col1 = Seq(col("column1"), col("column2"), col("column3"))
        val col2 = Seq(col("column1"), col("column2"))
        val col3 = Seq(col("column1"))

        assert(Functions.columnsToString(col1) == "column1, column2, column3")
        assert(Functions.columnsToString(col2) == "column1, column2")
        assert(Functions.columnsToString(col3) == "column1")

    }

    test("stringToDate should return valid value") {
        val format = "yyyy-MM-dd"
        val date = "2020-02-20"
        val javaDt = LocalDate.parse(date, DateTimeFormatter.ofPattern(format))

        assert(Functions.stringToDate(date) == javaDt)

    }

    test("dateToString should return valid value") {
        val format = "yyyy-MM-dd"
        val date = "2020-02-20"
        val javaDt = LocalDate.parse(date, DateTimeFormatter.ofPattern(format))

        assert(Functions.dateToString(javaDt) == date)

    }

    test("monthEnd should return valid value") {
        val format = "yyyy-MM-dd"
        def localDate(date: String) = LocalDate.parse(date, DateTimeFormatter.ofPattern(format))

        assert(Functions.monthEnd(localDate("2020-01-03")) == localDate("2020-01-31"))
        assert(Functions.monthEnd(localDate("2018-12-15")) == localDate("2018-12-31"))
        assert(Functions.monthEnd(localDate("2020-04-20")) == localDate("2020-04-30"))
        assert(Functions.monthEnd(localDate("2020-04-30")) == localDate("2020-04-30"))

    }

    test("monthBegin should return valid value") {
        val format = "yyyy-MM-dd"
        def localDate(date: String) = LocalDate.parse(date, DateTimeFormatter.ofPattern(format))

        assert(Functions.monthBegin(localDate("2020-01-03")) == localDate("2020-01-01"))
        assert(Functions.monthBegin(localDate("2018-12-15")) == localDate("2018-12-01"))
        assert(Functions.monthBegin(localDate("2020-04-20")) == localDate("2020-04-01"))
        assert(Functions.monthBegin(localDate("2020-04-30")) == localDate("2020-04-01"))

    }

    test("monthsPeriod should return valid value") {
        val format = "yyyy-MM-dd"
        def localDate(date: String) = LocalDate.parse(date, DateTimeFormatter.ofPattern(format))

        assert(Functions.monthsPeriod(localDate("2020-02-04"), localDate("2020-02-15")) == 1)
        assert(Functions.monthsPeriod(localDate("2020-01-04"), localDate("2020-02-15")) == 2)
        assert(Functions.monthsPeriod(localDate("2020-01-20"), localDate("2020-02-15")) == 2)
        assert(Functions.monthsPeriod(localDate("2019-11-04"), localDate("2020-02-15")) == 4)
        assert(Functions.monthsPeriod(localDate("2020-02-04"), localDate("2021-03-01")) == 14)

    }

    test("generateMonthsReportPeriods should return valid value") {

        val res1 = List(("2020-02-04", "2020-02-15"))
        val res2 = List(
            ("2020-01-04", "2020-01-31"),
            ("2020-02-01", "2020-02-15"))
        val res3 = List(
            ("2020-01-20", "2020-01-31"),
            ("2020-02-01", "2020-02-15"))
        val res4 = List(
            ("2019-11-04", "2019-11-30"),
            ("2019-12-01", "2019-12-31"),
            ("2020-01-01", "2020-01-31"),
            ("2020-02-01", "2020-02-15"))
        val res5 = List(
            ("2020-02-04", "2020-02-29"),
            ("2020-03-01", "2020-03-31"),
            ("2020-04-01", "2020-04-30"),
            ("2020-05-01", "2020-05-31"),
            ("2020-06-01", "2020-06-30"),
            ("2020-07-01", "2020-07-31"),
            ("2020-08-01", "2020-08-31"),
            ("2020-09-01", "2020-09-30"),
            ("2020-10-01", "2020-10-31"),
            ("2020-11-01", "2020-11-30"),
            ("2020-12-01", "2020-12-31"),
            ("2021-01-01", "2021-01-31"),
            ("2021-02-01", "2021-02-28"),
            ("2021-03-01", "2021-03-01"))

        assert(Functions.generateMonthsReportPeriods("2020-02-04", "2020-02-15") == res1)
        assert(Functions.generateMonthsReportPeriods("2020-01-04", "2020-02-15") == res2)
        assert(Functions.generateMonthsReportPeriods("2020-01-20", "2020-02-15") == res3)
        assert(Functions.generateMonthsReportPeriods("2019-11-04", "2020-02-15") == res4)
        assert(Functions.generateMonthsReportPeriods("2020-02-04", "2021-03-01") == res5)

    }

}
