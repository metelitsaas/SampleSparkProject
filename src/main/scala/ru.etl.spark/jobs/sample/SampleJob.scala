package ru.etl.spark.jobs.sample

import ru.etl.spark.utils.MainJob
import ru.etl.spark.utils.SparkSessionConfig
import ru.etl.spark.utils.Parameters
import ru.etl.spark.functions.Functions


object SampleJob extends MainJob {
    // Sample job
    override def run(sparkSessionConfig: SparkSessionConfig): Unit = {
        // Spark session and logger
        val spark = sparkSessionConfig.spark
        val logger = sparkSessionConfig.logger

        // Report date
        val reportBeginDate = Parameters.getParameter("reportBeginDate")
        val reportEndDate = Parameters.getParameter("reportEndDate")
        logger.info(f"Report begin date: $reportBeginDate")
        logger.info(f"Report end date: $reportEndDate")

        // Report periods
        val reportPeriods = Functions.generateMonthsReportPeriods(reportBeginDate, reportEndDate)
        logger.info(f"Report periods: ${reportPeriods.mkString(", ")}")

        // Sample job transformations class
        val st = new SampleTransforms(spark)

        logger.info(f"Transformations begin")

        reportPeriods.map { case (reportPeriodBegin, reportPeriodEnd) =>
            // Transformations
            st.loadCustomerTable // Load source table
                .transform(st.withMonthEnd) // Add month end column
                .transform(st.whereReportDt(reportPeriodBegin, reportPeriodEnd)) // Filter DataFrame
                .transform(st.joinCar) // Join car table
                .transform(st.withProcessedDttm) // Add processed timestamp
                .transform(st.repartitionByType("plan")) // Reduce number of partitions
        }

        logger.info(f"Transformations end")

        // Stop Spark session
        spark.stop()
        logger.info("SparkSession stopped")
    }

}