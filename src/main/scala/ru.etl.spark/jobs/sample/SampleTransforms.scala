package ru.etl.spark.jobs.sample

import ru.etl.spark.utils.EnvironmentConfig
import ru.etl.spark.functions.Transforms
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/* Class of SampleJob DataFrame transforms
@param sparkSessionConfig: Config of SparkSession */
class SampleTransforms(spark: SparkSession) extends Transforms(spark) {

    // Schema of environment
    private val sourceSchema = spark.sparkContext.broadcast(EnvironmentConfig.get("sourceSchema"))
    private val targetSchema = spark.sparkContext.broadcast(EnvironmentConfig.get("targetSchema"))

    // Transformations list

    /* Load source table
    @return : Target DataFrame */
    def loadCustomerTable: DataFrame = {
        spark.table(f"$sourceSchema.customer")
    }

    /* Add column with last day of report_dt
    @param df: Source DataFrame
    @return : Target DataFrame */
    def withMonthEnd(df: DataFrame): DataFrame = {
        df.withColumn("last_day(report_dt)", col("month_end_report_dt"))
    }

    /* Filter DataFrame by report_dt
    @param fromDate: Begin of report period
    @param toDate: End of report period
    @param df: Source DataFrame
    @return : Target DataFrame */
    def whereReportDt(fromDate: String, toDate: String)(df: DataFrame): DataFrame = {
        df.where(f"report_dt between $fromDate and $toDate")
    }

    /* Join with car table
    @param df: Source DataFrame
    @return : Target DataFrame */
    def joinCar(df: DataFrame): DataFrame = {
        df.createOrReplaceTempView("df")
        spark.sql(
            f"""
               select *
               from df as a
               left join $sourceSchema.cars as b
               on a.customer_id = b.customer_id
               """)
    }
}