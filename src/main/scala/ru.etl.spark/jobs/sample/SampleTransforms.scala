package ru.etl.spark.jobs.sample

import ru.etl.spark.utils.EnvironmentConfig
import ru.etl.spark.functions.{Loaders, Transforms}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/* Class of SampleJob DataFrame transforms
@param sparkSessionConfig: Config of SparkSession */
class SampleTransforms(spark: SparkSession) extends Transforms(spark) {

    // Schema of environment
    private val sourceSchema = EnvironmentConfig.get("sourceSchema")
    private val targetSchema = EnvironmentConfig.get("targetSchema")

    // Hive sources
    private val customerTable = spark.sparkContext.broadcast(f"$sourceSchema.customer")
    private val carsTable = spark.sparkContext.broadcast(f"$sourceSchema.cars")

    // Hive target
    private val dmTable = spark.sparkContext.broadcast(f"$sourceSchema.dm_final")

    // Transformations list
    /* Load source table
    @return : Target DataFrame */
    def readSourceTable(sourceName: String = f"${customerTable.value}"): DataFrame = {
        spark.table(sourceName)
    }

    /* Add column with last day of report_dt
    @param df: Source DataFrame
    @return : Target DataFrame */
    def withMonthEnd(df: DataFrame): DataFrame = {
        df.withColumn("month_end_report_dt", last_day(col("report_dt")))
    }

    /* Filter DataFrame by report_dt
    @param fromDate: Begin of report period
    @param toDate: End of report period
    @param df: Source DataFrame
    @return : Target DataFrame */
    def whereReportDt(fromDate: String, toDate: String)(df: DataFrame): DataFrame = {
        df.where(f"report_dt between '$fromDate' and '$toDate'")
    }

    /* Join with car table
    @param df: Source DataFrame
    @return : Target DataFrame */
    def joinCar(joinName: String = {carsTable.value})(df: DataFrame): DataFrame = {
        df.createOrReplaceTempView("df")
        spark.sql(
            f"""
                select
                    a.customer_id,
                    a.customer_nm,
                    b.car_nm
                from df as a
                left join $joinName as b
                    on a.customer_id = b.customer_id
                """)
    }

    /* Load target table
    @param df: Source DataFrame
    @return : Target DataFrame */
    def load(targetName: String = {dmTable.value})(df: DataFrame): DataFrame = {
        Loaders.overwriteTable(df, targetName)
        df
    }

}