package ru.etl.spark.functions

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

// General attribute functions
object Functions {

    /* Row's date and time of load
    @return : Current timestamp */
    def processedDttm: String = {
        val format = "yyyy-MM-dd HH:mm:ss"
        LocalDateTime
            .now()
            .format(DateTimeFormatter.ofPattern(format))
    }

    /* Split list of columns with ', ' delimiter to Columns
    @param str: String columns
    @return : List of Columns */
    def stringToColumns(str: String): Seq[Column] = {
        str.split(", ").map(x => col(x))
    }

    /* Concat Columns to string with ', ' delimiter
    @param columns: Sequence of Columns
    @return : String of columns */
    def columnsToString(columns: Seq[Column]): String = {
        columns.map(_.toString).mkString(", ")
    }

    /* Parse date string to LocalDate
    @param str: String of date
    @return : Date in LocalDate */
    def stringToDate(str: String): LocalDate = {
        LocalDate.parse(str, DateTimeFormatter.ISO_DATE)
    }

    /* Format LocalDate to date string
    @param date: Date in LocalDate
    @return : String of date */
    def dateToString(date: LocalDate): String = {
        date.format(DateTimeFormatter.ISO_DATE)
    }

    /* Get last day of month from date
    @param date: Date in LocalDate
    @return : Last day of month in LocalDate */
    def monthEnd(date: LocalDate): LocalDate = {
        date.withDayOfMonth(date.lengthOfMonth())
    }

    /* Get first of month from date
    @param date: Date in LocalDate
    @return : First day of month in LocalDate */
    def monthBegin(date: LocalDate): LocalDate = {
        date.withDayOfMonth(1)
    }

    /* Get months count between dates
    @param startDate: First date of period
    @param endDate: Last date of period
    @return : Count of months */
    def monthsPeriod(startDate: LocalDate, endDate: LocalDate): Int = {
        endDate.getMonth.getValue - startDate.getMonth.getValue + (endDate.getYear - startDate.getYear) * 12 + 1
    }

    /* Generate report periods by months based on start and end dates
    @param startStrDate: First date of period in String
    @param endStrDate: Last date of period in String
    @return : List of Tuple2 periods by month in String */
    def generateMonthsReportPeriods(startStrDate: String, endStrDate: String): List[(String, String)] = {
        val startDate = stringToDate(startStrDate)
        val endDate = stringToDate(endStrDate)

        // Count of months periods
        val monthsNum = monthsPeriod(startDate, endDate)

        val datesList = if (monthsNum > 1) { // More than one month period
            val startPeriod = List((startDate, monthEnd(startDate))) // First period
            val middlePeriods = List // List of internal periods
                .range(1, monthsNum - 1)
                .map(x => monthBegin(startDate).plusMonths(x))
                .map(x => (x, monthEnd(x)))
            val endPeriod = List((monthBegin(endDate), endDate)) // Last period

            startPeriod ++ middlePeriods ++ endPeriod
        } // When dates in one period
        else List((startDate, endDate))

        // Convert to String
        datesList.map{ case (begin, end) => (dateToString(begin), dateToString(end)) }

    }

}
